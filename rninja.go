package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"bufio"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	Builds []*Build
}

func ParseNinja(content string) (*NinjaFile, error) {
	nf := &NinjaFile{
		Rules:  make(map[string]*Rule),
		Builds: make([]*Build, 0),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
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
				nf.Builds = append(nf.Builds, build)
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

// Example usage:
func main() {
	ninjaContent := `rule clang_rule
    depfile = $out.d
    command = clang -g -Wall $
        -MMD -MF $out.d $
        -Wextra -Wno-sign-compare $
        -O2 -c -o $out $in

build .obj/examples/fib.o: clang_rule examples/fib.c`

	nf, err := ParseNinja(ninjaContent)
	if err != nil {
		panic(err)
	}

	// Access parsed data
	rule := nf.Rules["clang_rule"]
	build := nf.Builds[0]
	fmt.Printf("rule %v build %v", rule, build)
}

type BuildAction struct {
	Command     string
	InputFiles  []string
	OutputFiles []string
}

// func main() {
// 	// Example usage with ninja build file
// 	action := BuildAction{
// 		Command: "clang -g -Wall -MMD -MF .obj/quickjs.o.d -Wextra -Wno-sign-compare " +
// 			"-Wno-missing-field-initializers -Wundef -Wuninitialized -Wunused " +
// 			"-Wno-unused-parameter -Wwrite-strings -Wchar-subscripts -funsigned-char " +
// 			"-fwrapv -D_GNU_SOURCE -DCONFIG_VERSION=\"2024-02-14\" -DCONFIG_BIGNUM " +
// 			"-O2 -c -o .obj/quickjs.o quickjs.c",
// 		InputFiles:  []string{"quickjs.c"},
// 		OutputFiles: []string{".obj/quickjs.o", ".obj/quickjs.o.d"},
// 	}

// 	if err := executeBuildAction(action); err != nil {
// 		fmt.Printf("Build failed: %v\n", err)
// 		os.Exit(1)
// 	}
// }

func executeBuildAction(action BuildAction) error {
	ctx := context.Background()

	// Connect to remote cache server
	conn, err := grpc.Dial("127.0.0.1:8980", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to remote cache: %v", err)
	}
	defer conn.Close()

	// Create clients
	acClient := pb.NewActionCacheClient(conn)
	casClient := pb.NewContentAddressableStorageClient(conn)

	// Calculate action digest
	actionProto := &pb.Action{
		CommandDigest:   computeCommandDigest(action.Command),
		InputRootDigest: computeInputRootDigest(action.InputFiles),
	}
	actionBytes, err := proto.Marshal(actionProto)
	if err != nil {
		return fmt.Errorf("failed to marshal action: %v", err)
	}
	actionDigest := computeDigest(actionBytes)

	// Check if result exists in action cache
	req := &pb.GetActionResultRequest{
		InstanceName: "",
		ActionDigest: actionDigest,
	}
	result, err := acClient.GetActionResult(ctx, req)
	if err == nil {
		// Cache hit - download outputs
		for _, output := range result.OutputFiles {
			if err := downloadOutput(ctx, casClient, output); err != nil {
				return fmt.Errorf("failed to download output: %v", err)
			}
		}
		fmt.Println("Build outputs restored from cache")
		return nil
	}

	// Cache miss - execute the command
	fmt.Println("Cache miss, executing build command")
	if err := executeCommand(action.Command); err != nil {
		return fmt.Errorf("command execution failed: %v", err)
	}

	// Upload outputs to CAS and update action cache
	outputDigests := make([]*pb.OutputFile, 0, len(action.OutputFiles))
	for _, path := range action.OutputFiles {
		digest, err := uploadOutput(ctx, casClient, path)
		if err != nil {
			return fmt.Errorf("failed to upload output %s: %v", path, err)
		}
		outputDigests = append(outputDigests, &pb.OutputFile{
			Path:   path,
			Digest: digest,
		})
	}

	// Update action cache
	actionResult := &pb.ActionResult{
		OutputFiles: outputDigests,
	}
	updateReq := &pb.UpdateActionResultRequest{
		ActionDigest: actionDigest,
		ActionResult: actionResult,
	}
	if _, err := acClient.UpdateActionResult(ctx, updateReq); err != nil {
		return fmt.Errorf("failed to update action cache: %v", err)
	}

	return nil
}

func executeCommand(command string) error {
	parts := strings.Fields(command)
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func computeDigest(data []byte) *pb.Digest {
	hash := sha256.Sum256(data)
	return &pb.Digest{
		Hash:      hex.EncodeToString(hash[:]),
		SizeBytes: int64(len(data)),
	}
}

func computeCommandDigest(command string) *pb.Digest {
	cmd := &pb.Command{
		Arguments: strings.Fields(command),
	}
	data, _ := proto.Marshal(cmd)
	return computeDigest(data)
}

func computeInputRootDigest(inputs []string) *pb.Digest {
	// Create Directory message with input files
	dir := &pb.Directory{}
	for _, path := range inputs {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		digest := computeDigest(data)
		dir.Files = append(dir.Files, &pb.FileNode{
			Name:   filepath.Base(path),
			Digest: digest,
		})
	}
	data, _ := proto.Marshal(dir)
	return computeDigest(data)
}

func uploadOutput(ctx context.Context, client pb.ContentAddressableStorageClient, path string) (*pb.Digest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	digest := computeDigest(data)
	req := &pb.BatchUpdateBlobsRequest{
		Requests: []*pb.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest,
				Data:   data,
			},
		},
	}

	if _, err := client.BatchUpdateBlobs(ctx, req); err != nil {
		return nil, err
	}
	return digest, nil
}

func downloadOutput(ctx context.Context, client pb.ContentAddressableStorageClient, output *pb.OutputFile) error {
	req := &pb.BatchReadBlobsRequest{
		Digests: []*pb.Digest{output.Digest},
	}

	resp, err := client.BatchReadBlobs(ctx, req)
	if err != nil {
		return err
	}

	if len(resp.Responses) == 0 {
		return fmt.Errorf("no blob data received")
	}

	if resp.Responses[0].Status.Code != 0 {
		return fmt.Errorf("failed to download blob: %v", resp.Responses[0].Status.Message)
	}

	// Ensure output directory exists
	if dir := filepath.Dir(output.Path); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return os.WriteFile(output.Path, resp.Responses[0].Data, 0644)
}
