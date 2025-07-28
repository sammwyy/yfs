package main

import (
	"bufio"
	"flag"
	"fmt"

	"log"
	"os"

	"github.com/sammwyy/yfs"
)

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
		fs, err = yfs.New(*directory)
	} else if *indexFile != "" && *freeFile != "" && *blocksFile != "" {
		fs, err = yfs.NewFromPaths(*indexFile, *freeFile, *blocksFile)
	} else {
		fmt.Fprintf(os.Stderr, "Error: Must specify either -dir or all three files (-index, -free, -blocks)\n")
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("Failed to initialize YFS: %v", err)
	}

	cli := &Root{
		fs:          fs,
		currentPath: "/",
		scanner:     bufio.NewScanner(os.Stdin),
	}

	cli.run()
}
