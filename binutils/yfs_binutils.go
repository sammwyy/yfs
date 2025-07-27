package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	yfs "github.com/sammwyy/yfs/lib"
)

type YFSCli struct {
	fs          *yfs.YFS
	currentPath string
	scanner     *bufio.Scanner
}

func main() {
	var (
		directory  = flag.String("dir", "", "Directory containing YFS files (index.yfs, free.yfs, blocks.glob)")
		indexFile  = flag.String("index", "", "Path to index.yfs file")
		freeFile   = flag.String("free", "", "Path to free.yfs file")
		blocksFile = flag.String("blocks", "", "Path to blocks.glob file")
		help       = flag.Bool("h", false, "Show help")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "YFS CLI Tool - Interactive command line interface for YFS file system\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s -dir <directory>                    # Use directory containing YFS files\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -index <file> -free <file> -blocks <file>  # Specify individual files\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nCommands available in interactive mode:\n")
		fmt.Fprintf(os.Stderr, "  ls [path]                   - List directory contents\n")
		fmt.Fprintf(os.Stderr, "  cd <path>                   - Change current directory\n")
		fmt.Fprintf(os.Stderr, "  pwd                         - Print current directory\n")
		fmt.Fprintf(os.Stderr, "  cat <file>                  - Display file contents\n")
		fmt.Fprintf(os.Stderr, "  cp <src> <dst>              - Copy file within YFS\n")
		fmt.Fprintf(os.Stderr, "  mv <src> <dst>              - Move/rename file within YFS\n")
		fmt.Fprintf(os.Stderr, "  rm <file>                   - Delete file\n")
		fmt.Fprintf(os.Stderr, "  mkdir <dir>                 - Create directory (creates parent dirs if needed)\n")
		fmt.Fprintf(os.Stderr, "  write <file> <content>      - Write content to file\n")
		fmt.Fprintf(os.Stderr, "  push <local_file> <remote_file>  - Copy local file to YFS\n")
		fmt.Fprintf(os.Stderr, "  pull <remote_file> <local_file>  - Copy YFS file to local filesystem\n")
		fmt.Fprintf(os.Stderr, "  tree                        - Show complete directory tree\n")
		fmt.Fprintf(os.Stderr, "  stats                       - Show filesystem statistics\n")
		fmt.Fprintf(os.Stderr, "  help                        - Show this help\n")
		fmt.Fprintf(os.Stderr, "  exit, quit                  - Exit the CLI\n")
	}

	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	var fs *yfs.YFS
	var err error

	// Initialize YFS based on provided arguments
	if *directory != "" {
		if *indexFile != "" || *freeFile != "" || *blocksFile != "" {
			fmt.Fprintf(os.Stderr, "Error: Cannot use -dir with individual file flags\n")
			flag.Usage()
			os.Exit(1)
		}
		fs, err = yfs.NewYFS(*directory)
	} else if *indexFile != "" && *freeFile != "" && *blocksFile != "" {
		fs, err = yfs.NewYFSFromPaths(*indexFile, *freeFile, *blocksFile)
	} else {
		fmt.Fprintf(os.Stderr, "Error: Must specify either -dir or all three files (-index, -free, -blocks)\n")
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Failed to initialize YFS: %v", err)
	}

	cli := &YFSCli{
		fs:          fs,
		currentPath: "/",
		scanner:     bufio.NewScanner(os.Stdin),
	}

	cli.run()
}

func (c *YFSCli) run() {
	fmt.Println("YFS CLI Tool")
	fmt.Printf("Block size: %d bytes\n", c.fs.GetBlockSize())
	fmt.Println("Type 'help' for available commands or 'exit' to quit")
	fmt.Println()

	for {
		fmt.Printf("yfs:%s$ ", c.currentPath)

		if !c.scanner.Scan() {
			break
		}

		line := strings.TrimSpace(c.scanner.Text())
		if line == "" {
			continue
		}

		args := parseCommand(line)
		if len(args) == 0 {
			continue
		}

		command := args[0]
		args = args[1:]

		switch command {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return
		case "help":
			c.showHelp()
		case "ls":
			c.cmdLs(args)
		case "cd":
			c.cmdCd(args)
		case "pwd":
			c.cmdPwd()
		case "cat":
			c.cmdCat(args)
		case "cp":
			c.cmdCp(args)
		case "mv":
			c.cmdMv(args)
		case "rm":
			c.cmdRm(args)
		case "mkdir":
			c.cmdMkdir(args)
		case "write":
			c.cmdWrite(args)
		case "push":
			c.cmdPush(args)
		case "pull":
			c.cmdPull(args)
		case "tree":
			c.cmdTree()
		case "stats":
			c.cmdStats()
		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
		}
	}
}

func (c *YFSCli) showHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  ls [path]                   - List directory contents")
	fmt.Println("  cd <path>                   - Change current directory")
	fmt.Println("  pwd                         - Print current directory")
	fmt.Println("  cat <file>                  - Display file contents")
	fmt.Println("  cp <src> <dst>              - Copy file within YFS")
	fmt.Println("  mv <src> <dst>              - Move/rename file within YFS")
	fmt.Println("  rm <file>                   - Delete file")
	fmt.Println("  mkdir <dir>                 - Create directory")
	fmt.Println("  write <file> <content>      - Write content to file")
	fmt.Println("  push <local_file> <remote_file>  - Copy local file to YFS")
	fmt.Println("  pull <remote_file> <local_file>  - Copy YFS file to local filesystem")
	fmt.Println("  tree                        - Show complete directory tree")
	fmt.Println("  stats                       - Show filesystem statistics")
	fmt.Println("  help                        - Show this help")
	fmt.Println("  exit, quit                  - Exit the CLI")
}

func (c *YFSCli) cmdLs(args []string) {
	path := c.currentPath
	if len(args) > 0 {
		path = c.resolvePath(args[0])
	}

	entries, err := c.fs.Ls(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(entries) == 0 {
		fmt.Println("Directory is empty")
		return
	}

	// Sort entries: directories first, then files
	dirs, files := []yfs.FileEntry{}, []yfs.FileEntry{}
	for _, entry := range entries {
		if entry.IsDirectory {
			dirs = append(dirs, entry)
		} else {
			files = append(files, entry)
		}
	}

	for _, entry := range dirs {
		fmt.Printf("d %s %s/\n",
			entry.ModTime.Format("2006-01-02 15:04:05"), entry.Name)
	}

	for _, entry := range files {
		fmt.Printf("- %s %8d %s\n",
			entry.ModTime.Format("2006-01-02 15:04:05"), entry.Size, entry.Name)
	}
}

func (c *YFSCli) cmdCd(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: cd <path>")
		return
	}

	newPath := c.resolvePath(args[0])

	// Check if the path exists and is a directory
	entries, err := c.fs.Ls(newPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// If we can list it, it's a valid directory
	_ = entries
	c.currentPath = newPath
}

func (c *YFSCli) cmdPwd() {
	fmt.Println(c.currentPath)
}

func (c *YFSCli) cmdCat(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: cat <file>")
		return
	}

	path := c.resolvePath(args[0])
	data, err := c.fs.ReadFile(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Print(string(data))
	if !strings.HasSuffix(string(data), "\n") {
		fmt.Println()
	}
}

func (c *YFSCli) cmdCp(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: cp <src> <dst>")
		return
	}

	src := c.resolvePath(args[0])
	dst := c.resolvePath(args[1])

	err := c.fs.CopyFile(src, dst)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Copied %s to %s\n", src, dst)
}

func (c *YFSCli) cmdMv(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: mv <src> <dst>")
		return
	}

	src := c.resolvePath(args[0])
	dst := c.resolvePath(args[1])

	err := c.fs.MoveFile(src, dst)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Moved %s to %s\n", src, dst)
}

func (c *YFSCli) cmdRm(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: rm <file>")
		return
	}

	path := c.resolvePath(args[0])
	err := c.fs.DeleteFile(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Deleted %s\n", path)
}

func (c *YFSCli) cmdMkdir(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: mkdir <dir>")
		return
	}

	path := c.resolvePath(args[0])

	// Create a dummy file in the directory to ensure it exists
	// This is a workaround since YFS might not have explicit directory creation
	dummyFile := filepath.Join(path, ".keep")
	err := c.fs.WriteFile(dummyFile, []byte(""))
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}

	// Remove the dummy file
	c.fs.DeleteFile(dummyFile)
	fmt.Printf("Created directory %s\n", path)
}

func (c *YFSCli) cmdWrite(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: write <file> <content>")
		return
	}

	path := c.resolvePath(args[0])
	content := strings.Join(args[1:], " ")

	err := c.fs.WriteFile(path, []byte(content))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Wrote %d bytes to %s\n", len(content), path)
}

func (c *YFSCli) cmdPush(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: push <local_file> <remote_file>")
		return
	}

	localFile := args[0]
	remotePath := c.resolvePath(args[1])

	// Read local file
	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		fmt.Printf("Error reading local file: %v\n", err)
		return
	}

	// Write to YFS
	err = c.fs.WriteFile(remotePath, data)
	if err != nil {
		fmt.Printf("Error writing to YFS: %v\n", err)
		return
	}

	fmt.Printf("Pushed %s (%d bytes) to %s\n", localFile, len(data), remotePath)
}

func (c *YFSCli) cmdPull(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: pull <remote_file> <local_file>")
		return
	}

	remotePath := c.resolvePath(args[0])
	localFile := args[1]

	// Read from YFS
	data, err := c.fs.ReadFile(remotePath)
	if err != nil {
		fmt.Printf("Error reading from YFS: %v\n", err)
		return
	}

	// Write to local file
	err = ioutil.WriteFile(localFile, data, 0644)
	if err != nil {
		fmt.Printf("Error writing local file: %v\n", err)
		return
	}

	fmt.Printf("Pulled %s (%d bytes) to %s\n", remotePath, len(data), localFile)
}

func (c *YFSCli) cmdTree() {
	tree, err := c.fs.LsAll()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	c.printTree(tree, "")
}

func (c *YFSCli) cmdStats() {
	stats, err := c.fs.GetStats()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Filesystem Statistics:")
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}
}

func (c *YFSCli) printTree(entry *yfs.FileEntry, indent string) {
	if entry.IsDirectory {
		fmt.Printf("%s├── %s/\n", indent, entry.Name)

		for name, child := range entry.Children {
			if child.IsDirectory {
				fmt.Printf("%s├── %s/\n", indent, name)
				c.printTree(child, indent+"│   ")
			} else {
				fmt.Printf("%s├── %s (%d bytes)\n", indent, name, child.Size)
			}
		}
	} else {
		fmt.Printf("%s├── %s (%d bytes)\n", indent, entry.Name, entry.Size)
	}
}

func (c *YFSCli) resolvePath(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}

	if path == "." {
		return c.currentPath
	}

	if path == ".." {
		if c.currentPath == "/" {
			return "/"
		}
		parts := strings.Split(strings.Trim(c.currentPath, "/"), "/")
		if len(parts) <= 1 {
			return "/"
		}
		return "/" + strings.Join(parts[:len(parts)-1], "/")
	}

	if c.currentPath == "/" {
		return "/" + path
	}
	return c.currentPath + "/" + path
}

func parseCommand(line string) []string {
	var args []string
	var current strings.Builder
	inQuotes := false
	escaped := false

	for _, r := range line {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			inQuotes = !inQuotes
		case r == ' ' && !inQuotes:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}
