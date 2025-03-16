package freelist

import (
	"errors"
)

type FreeList interface {
	// Attempt allocating number of pages requested
	GetPages(count uint64) ([]uint64, error)
	// Release pages back to the free list
	ReleasePages(pages []uint64) error
	FreePagesAvailable() uint64 // Returns the number of free pages available
	CurrentAddressRange() [2]uint64
}

type BitmapFreeList struct {
	bitmap    []byte
	next      []int  // Linked list of free pages
	head      int    // Head of the free list
	start     uint64 // Start of the address range
	end       uint64 // End of the address range
	freePages uint64 // Counter for free pages available
}

func (fl *BitmapFreeList) FreePagesAvailable() uint64 {
	return fl.freePages
}

func (fl *BitmapFreeList) GetPages(count uint64) ([]uint64, error) {
	if count == 0 {
		return nil, nil
	}

	var pages []uint64
	current := fl.head

	for i := uint64(0); i < count; i++ {
		if current == -1 {
			break
		}
		pages = append(pages, uint64(current))
		fl.bitmap[current/8] |= 1 << (current % 8) // Mark page as allocated
		prev := current
		current = fl.next[current] // Move to the next free page
		fl.next[prev] = -1         // Remove the allocated page from the free list
		fl.freePages--             // Decrement the free pages counter
	}

	fl.head = current // Update the head to the new top of the free list

	return pages, nil
}

func (fl *BitmapFreeList) ReleasePages(pages []uint64) error {
	if len(pages) == 0 {
		return nil
	}

	for _, page := range pages {
		// Check if the page is within the valid range
		if page < fl.start || page >= fl.end {
			return errors.New("page is outside the valid address range")
		}

		// Check if the page is already free
		if fl.bitmap[page/8]&(1<<(page%8)) == 0 {
			continue
		}

		fl.bitmap[page/8] &^= 1 << (page % 8) // Mark page as free
		fl.next[page] = fl.head               // Point the released page to the current head
		fl.head = int(page)                   // Make the released page the new head
		fl.freePages++                        // Increment the free pages counter
	}

	return nil
}

func (fl *BitmapFreeList) CurrentAddressRange() [2]uint64 {
	return [2]uint64{fl.start, fl.end}
}

/*
 Cache friendly free list implementation
 - Bitmap for serialization and markers
 - Linked list for fast allocation and deallocation
*/

func NewBitmapFreeList(bitmap []byte, start, end uint64) FreeList {
	size := len(bitmap) * 8
	next := make([]int, size)
	head := -1             // Start with no free pages
	prev := -1             // Track the previous free page
	freePages := uint64(0) // Counter for free pages

	// Initialize the linked list of free pages based on the bitmap
	for i := 0; i < size; i++ {
		page := uint64(i)
		if page < start || page >= end {
			next[i] = -1 // Mark as out of range
			continue
		}

		if bitmap[i/8]&(1<<(i%8)) == 0 { // Check if the page is free
			if head == -1 {
				head = i // Set the head to the first free page
			} else {
				next[prev] = i // Point the previous free page to the current free page
			}
			prev = i    // Update the previous free page
			freePages++ // Increment the free pages counter
		} else {
			next[i] = -1 // Mark as allocated
		}
	}

	// Ensure the last free page in the range points to -1
	if prev != -1 {
		next[prev] = -1 // Terminate the linked list
	}

	return &BitmapFreeList{
		bitmap:    bitmap,
		next:      next,
		head:      head,
		start:     start,
		end:       end,
		freePages: freePages, // Initialize the free pages counter
	}
}
