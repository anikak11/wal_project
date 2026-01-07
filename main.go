package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"wal_project/wal"
)

func main() {
	// 1. Setup temporary directory for the WAL
	tmpDir, err := os.MkdirTemp("", "wal_demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "demo.wal")
	fmt.Printf("--- Phase 1: Creating WAL and Writing Data ---\n")
	fmt.Printf("File: %s\n\n", walPath)

	// 2. Initialize the WAL
	w, err := wal.New(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}

	// 3. Append some entries
	entries := []string{
		"Set user_1=active",
		"Update user_1_score=100",
		"Delete session_99",
	}

	for i, data := range entries {
		err := w.AppendAndSync([]byte(data))
		if err != nil {
			log.Fatalf("Failed to append: %v", err)
		}
		fmt.Printf("Appended Entry %d: %s\n", i+1, data)
	}

	// 4. Verify the LastIndex
	fmt.Printf("\nCurrent Last Index: %d\n", w.LastIndex())

	// 5. Close the WAL (simulating a shutdown)
	w.Close()
	fmt.Printf("\n--- Phase 2: Simulating Restart & Recovery ---\n")

	// 6. Re-open the same file
	// The New() function will trigger initialize() -> recover()
	w2, err := wal.New(walPath)
	if err != nil {
		log.Fatalf("Failed to recover WAL: %v", err)
	}
	defer w2.Close()

	// 7. Read all entries back to prove they survived
	recovered, err := w2.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read all: %v", err)
	}

	fmt.Printf("Recovered %d entries from disk:\n", len(recovered))
	for i, data := range recovered {
		fmt.Printf("  [%d] %s\n", i+1, string(data))
	}

	// 8. Demonstrate Truncation (Raft Conflict Simulation)
	fmt.Printf("\n--- Phase 3: Truncating from Index 2 ---\n")
	err = w2.TruncateFromIndex(2) // Keeps index 1, deletes 2 and 3
	if err != nil {
		log.Fatalf("Truncate failed: %v", err)
	}

	fmt.Printf("New Last Index after truncation: %d\n", w2.LastIndex())
	
	finalEntries, _ := w2.ReadAll()
	fmt.Printf("Final Log Content:\n")
	for i, data := range finalEntries {
		fmt.Printf("  [%d] %s\n", i+1, string(data))
	}
}