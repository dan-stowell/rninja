package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Rule struct {
	Name    string
	Depfile string
	Command string
}

type Build struct {
	Outputs []string
	Rule    string
	Inputs  []string
}

type NinjaFile struct {
	Rules  map[string]*Rule
	Builds map[string]*Build
}

func ParseNinja(r io.Reader) (*NinjaFile, error) {
	nf := &NinjaFile{
		Rules:  make(map[string]*Rule),
		Builds: make(map[string]*Build),
	}

	scanner := bufio.NewScanner(r)
	var currentRule *Rule
	var continuedLine string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Handle line continuations
		if strings.HasSuffix(line, "$") {
			continuedLine += strings.TrimSuffix(line, "$")
			continue
		}
		if continuedLine != "" {
			line = continuedLine + line
			continuedLine = ""
		}

		// Parse rules
		if strings.HasPrefix(line, "rule ") {
			ruleName := strings.TrimPrefix(line, "rule ")
			currentRule = &Rule{Name: ruleName}
			nf.Rules[ruleName] = currentRule
			continue
		}

		// Parse rule properties
		if currentRule != nil && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			switch key {
			case "depfile":
				currentRule.Depfile = value
			case "command":
				currentRule.Command = value
			}
			continue
		}

		// Parse build statements
		if strings.HasPrefix(line, "build ") {
			build := parseBuildLine(line)
			if build != nil {
				for _, output := range build.Outputs {
					nf.Builds[output] = build
				}
			}
			currentRule = nil
		}
	}

	return nf, scanner.Err()
}

func parseBuildLine(line string) *Build {
	// Remove "build " prefix
	line = strings.TrimPrefix(line, "build ")

	// Split into outputs and rule+inputs
	parts := strings.Split(line, ":")
	if len(parts) != 2 {
		return nil
	}

	outputs := strings.Fields(parts[0])
	ruleAndInputs := strings.Fields(parts[1])

	if len(ruleAndInputs) < 1 {
		return nil
	}

	return &Build{
		Outputs: outputs,
		Rule:    ruleAndInputs[0],
		Inputs:  ruleAndInputs[1:],
	}
}

// // Example usage:
// func main() {
// 	// 	ninjaContent := `rule clang_rule
// 	//     depfile = $out.d
// 	//     command = clang -g -Wall $
// 	//         -MMD -MF $out.d $
// 	//         -Wextra -Wno-sign-compare $
// 	//         -O2 -c -o $out $in

// 	// build .obj/examples/fib.o: clang_rule examples/fib.c`

// 	r, err := os.Open("build.ninja")
// 	if err != nil {
// 		panic(err)
// 	}
// 	nf, err := ParseNinja(r)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Access parsed data
// 	fmt.Printf("rule count %d build count %d", len(nf.Rules), len(nf.Builds))
// }

type RemoteCache struct {
	client      remote.ExecutionClient
	actionCache remote.ActionCacheClient
	cas         remote.ContentAddressableStorageClient
}

func NewRemoteCache(conn *grpc.ClientConn) *RemoteCache {
	return &RemoteCache{
		client:      remote.NewExecutionClient(conn),
		actionCache: remote.NewActionCacheClient(conn),
		cas:         remote.NewContentAddressableStorageClient(conn),
	}
}

func computeActionDigest(action *remote.Action) (*remote.Digest, error) {
	data, err := proto.Marshal(action)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Action: %v", err)
	}

	hash := sha256.Sum256(data)
	return &remote.Digest{
		Hash:      hex.EncodeToString(hash[:]),
		SizeBytes: int64(len(data)),
	}, nil
}

func (rc *RemoteCache) uploadBlob(ctx context.Context, data []byte) (*remote.Digest, error) {
	hash := sha256.Sum256(data)
	digest := &remote.Digest{
		Hash:      hex.EncodeToString(hash[:]),
		SizeBytes: int64(len(data)),
	}

	// Check if blob already exists
	resp, err := rc.cas.FindMissingBlobs(ctx, &remote.FindMissingBlobsRequest{
		BlobDigests: []*remote.Digest{digest},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check blob existence: %v", err)
	}

	if len(resp.MissingBlobDigests) > 0 {
		// Upload if missing
		_, err = rc.cas.BatchUpdateBlobs(ctx, &remote.BatchUpdateBlobsRequest{
			Requests: []*remote.BatchUpdateBlobsRequest_Request{{
				Digest: digest,
				Data:   data,
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to upload blob: %v", err)
		}
	}

	return digest, nil
}

func (rc *RemoteCache) downloadBlob(ctx context.Context, digest *remote.Digest) ([]byte, error) {
	resp, err := rc.cas.BatchReadBlobs(ctx, &remote.BatchReadBlobsRequest{
		Digests: []*remote.Digest{digest},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download blob: %v", err)
	}

	if len(resp.Responses) != 1 {
		return nil, fmt.Errorf("unexpected number of responses: %d", len(resp.Responses))
	}

	if resp.Responses[0].Status.Code != 0 {
		return nil, fmt.Errorf("failed to read blob: %v", resp.Responses[0].Status.Message)
	}

	return resp.Responses[0].Data, nil
}

func expandVariables(command string, in []string, out []string) string {
	command = strings.ReplaceAll(command, "${in}", strings.Join(in, " "))
	command = strings.ReplaceAll(command, "${out}", strings.Join(out, " "))
	command = strings.ReplaceAll(command, "$in", strings.Join(in, " "))
	command = strings.ReplaceAll(command, "$out", strings.Join(out, " "))
	return command
}

func (rc *RemoteCache) processNinjaBuild(ctx context.Context, build *Build, rule *Rule) error {
	log.Printf("processNinjaBuild %v", build)
	// Create Action from build and rule
	action := &remote.Action{
		CommandDigest:   &remote.Digest{}, // We'll set this after uploading command
		InputRootDigest: &remote.Digest{}, // We'll set this after uploading inputs
		DoNotCache:      false,
	}

	expandedCommand := expandVariables(rule.Command, build.Inputs, build.Outputs)
	// Create Command from rule
	command := &remote.Command{
		Arguments:   strings.Fields(expandedCommand),
		OutputFiles: build.Outputs,
	}

	// Upload command
	commandData, err := proto.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}

	commandDigest, err := rc.uploadBlob(ctx, commandData)
	if err != nil {
		return fmt.Errorf("failed to upload command: %v", err)
	}
	action.CommandDigest = commandDigest

	// Create input root directory
	inputRoot := &remote.Directory{}
	for _, input := range build.Inputs {
		data, err := os.ReadFile(input)
		if err != nil {
			return fmt.Errorf("failed to read input %s: %v", input, err)
		}

		digest, err := rc.uploadBlob(ctx, data)
		if err != nil {
			return fmt.Errorf("failed to upload input %s: %v", input, err)
		}

		inputRoot.Files = append(inputRoot.Files, &remote.FileNode{
			Name:   filepath.Base(input),
			Digest: digest,
		})
	}

	// Upload input root
	inputRootData, err := proto.Marshal(inputRoot)
	if err != nil {
		return fmt.Errorf("failed to marshal input root: %v", err)
	}

	inputRootDigest, err := rc.uploadBlob(ctx, inputRootData)
	if err != nil {
		return fmt.Errorf("failed to upload input root: %v", err)
	}
	action.InputRootDigest = inputRootDigest

	// Compute action digest
	actionDigest, err := computeActionDigest(action)
	if err != nil {
		return fmt.Errorf("failed to compute action digest: %v", err)
	}

	// Check action cache
	result, err := rc.actionCache.GetActionResult(ctx, &remote.GetActionResultRequest{
		ActionDigest: actionDigest,
	})
	if err == nil {
		// Cache hit - download outputs
		for _, output := range result.OutputFiles {
			data, err := rc.downloadBlob(ctx, output.Digest)
			if err != nil {
				return fmt.Errorf("failed to download output %s: %v", output.Path, err)
			}

			if err := os.WriteFile(output.Path, data, 0644); err != nil {
				return fmt.Errorf("failed to write output %s: %v", output.Path, err)
			}
		}
		return nil
	}

	// Cache miss - execute command locally
	cmd := exec.CommandContext(ctx, command.Arguments[0], command.Arguments[1:]...)
	log.Printf("=== %s", cmd)
	cmd.Stdout = os.Stdout // Capture output
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("command execution failed: %v", err)
	}

	// Create ActionResult
	actionResult := &remote.ActionResult{}
	for _, output := range build.Outputs {
		data, err := os.ReadFile(output)
		if err != nil {
			return fmt.Errorf("failed to read output %s: %v", output, err)
		}

		digest, err := rc.uploadBlob(ctx, data)
		if err != nil {
			return fmt.Errorf("failed to upload output %s: %v", output, err)
		}

		actionResult.OutputFiles = append(actionResult.OutputFiles, &remote.OutputFile{
			Path:   output,
			Digest: digest,
		})
	}

	// Update action cache
	_, err = rc.actionCache.UpdateActionResult(ctx, &remote.UpdateActionResultRequest{
		ActionDigest: actionDigest,
		ActionResult: actionResult,
	})
	if err != nil {
		return fmt.Errorf("failed to update action cache: %v", err)
	}

	return nil
}

func resolveDependencies(nf *NinjaFile, target string) []*Build {
	visited := make(map[string]bool)
	var builds []*Build

	var resolve func(output string)
	resolve = func(output string) {
		if visited[output] {
			return
		}
		// visited[output] = true

		if build, exists := nf.Builds[output]; exists {
			log.Printf("output %s appending %v", output, build.Inputs)
			builds = append(builds, build)
			for _, input := range build.Inputs {
				resolve(input)
			}
		}
	}

	resolve(target)

	// Reverse the builds slice
	for i := 0; i < len(builds)/2; i++ {
		j := len(builds) - 1 - i
		builds[i], builds[j] = builds[j], builds[i]
	}

	return builds
}

func processNinjaFile(ctx context.Context, target string, nf *NinjaFile, remoteCache *RemoteCache) error {
	if _, exists := nf.Builds[target]; exists {
		buildsInDependencyOrder := resolveDependencies(nf, target)
		for _, build := range buildsInDependencyOrder {
			rule := nf.Rules[build.Rule]
			if rule == nil {
				return fmt.Errorf("undefined rule: %s", build.Rule)
			}
			if err := remoteCache.processNinjaBuild(ctx, build, rule); err != nil {
				return fmt.Errorf("failed to process build %v: %v", build.Outputs, err)
			}
		}
	}
	return nil
}

func main() {
	// Set up connection to remote execution service
	conn, err := grpc.Dial("localhost:8980", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	remoteCache := NewRemoteCache(conn)

	// Example ninja file
	r, err := os.Open("build.ninja")
	if err != nil {
		panic(err)
	}
	nf, err := ParseNinja(r)
	if err != nil {
		panic(err)
	}

	if err := processNinjaFile(context.Background(), "qjs", nf, remoteCache); err != nil {
		fmt.Fprintf(os.Stderr, "Build failed: %v\n", err)
		os.Exit(1)
	}
}
