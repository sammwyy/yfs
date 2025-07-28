package main

import (
	"bufio"
	"fmt"

	"os"
	"strings"

	"github.com/sammwyy/yfs"
)

type Root struct {
	fs          *yfs.YFS
	currentPath string
	scanner     *bufio.Scanner
}

func (c *Root) run() {
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

func (c *Root) showHelp() {
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

func (c *Root) cmdLs(args []string) {
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
	for _, entry := range entries {
		if entry.IsDirectory {
			fmt.Printf("d %s %s/\n",
				entry.ModTime.Format("2006-01-02 15:04:05"), entry.Name)
		} else {
			fmt.Printf("- %s %8d %s\n",
				entry.ModTime.Format("2006-01-02 15:04:05"), entry.Size, entry.Name)
		}
	}
}

func (c *Root) cmdCd(args []string) {
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

func (c *Root) cmdPwd() {
	fmt.Println(c.currentPath)
}

func (c *Root) cmdCat(args []string) {
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

func (c *Root) cmdCp(args []string) {
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

func (c *Root) cmdMv(args []string) {
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

func (c *Root) cmdRm(args []string) {
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

func (c *Root) cmdMkdir(args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: mkdir <dir>")
		return
	}

	path := c.resolvePath(args[0])
	c.fs.CreateDirectory(path)

	fmt.Printf("Created directory %s\n", path)
}

func (c *Root) cmdWrite(args []string) {
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

func (c *Root) cmdPush(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: push <local_file> <remote_file>")
		return
	}

	localFile := args[0]
	remotePath := c.resolvePath(args[1])

	// Read local file
	data, err := os.ReadFile(localFile)
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

func (c *Root) cmdPull(args []string) {
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
	err = os.WriteFile(localFile, data, 0644)
	if err != nil {
		fmt.Printf("Error writing local file: %v\n", err)
		return
	}

	fmt.Printf("Pulled %s (%d bytes) to %s\n", remotePath, len(data), localFile)
}

func (c *Root) cmdTree() {
	tree, err := c.fs.LsAll()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	c.printTree(tree, "")
}

func (c *Root) cmdStats() {
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

func (c *Root) printFile(entry *yfs.FileEntry, indent string) {
	fmt.Printf("%s├── %s (%d bytes)\n", indent, entry.Metadata.Name, entry.Size)
}

func (c *Root) printTree(entry *yfs.FileInfo, indent string) {
	if entry.IsDirectory {
		fmt.Printf("%s├── %s/\n", indent, entry.Name)
	} else {
		fmt.Printf("%s├── %s (%d bytes)\n", indent, entry.Name, entry.Size)
	}
}

func (c *Root) resolvePath(path string) string {
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
