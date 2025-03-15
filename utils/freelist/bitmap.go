package freelist

import (
	"errors"
)

type FreeList interface {
	// Attempt allocating number of pages requested
	GetPages(count uint64) ([]uint64, error)
	// Release pages back to the free list
	ReleasePages(pages []uint64) error
	LastModifiedRange() [2]uint64
	FreePageAvailable() bool
}

type BitmapFreeList struct {
	bitmap        []byte
	next          []int // Linked list of free pages
	head          int   // Head of the free list
	modifiedRange [2]uint64
	start         uint64 // Start of the address range
	end           uint64 // End of the address range
}

func (fl *BitmapFreeList) FreePageAvailable() bool {
	return fl.head != -1
}

func (fl *BitmapFreeList) GetPages(count uint64) ([]uint64, error) {
	if count == 0 {
		return nil, nil
	}

	var pages []uint64
	current := fl.head
	if current == -1 {
		return nil, errors.New("not enough free pages available")
	}

	for i := uint64(0); i < count; i++ {
		if current == -1 {
			break
		}
		pages = append(pages, uint64(current))
		fl.bitmap[current/8] |= 1 << (current % 8) // Mark page as allocated
		prev := current
		current = fl.next[current] // Move to the next free page
		fl.next[prev] = -1         // Remove the allocated page from the free list
	}

	fl.head = current             // Update the head to the new top of the free list
	fl.updateModifiedRange(pages) // Update modified range with allocated pages
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
	}

	fl.updateModifiedRange(pages) // Update modified range with released pages
	return nil
}

func (fl *BitmapFreeList) LastModifiedRange() [2]uint64 {
	return fl.modifiedRange
}

func (fl *BitmapFreeList) updateModifiedRange(pages []uint64) {
	if len(pages) == 0 {
		return
	}

	// Find the minimum (low) and maximum (high) page numbers
	low := pages[0]
	high := pages[0]
	for _, page := range pages {
		if page < low {
			low = page
		}
		if page > high {
			high = page
		}
	}

	// Update the modified range
	if fl.modifiedRange[0] > low {
		fl.modifiedRange[0] = low
	}
	if fl.modifiedRange[1] < high {
		fl.modifiedRange[1] = high
	}
}

/*
 Cache friendly free list implementation
 - Bitmap for serialization and markers
 - Linked list for fast allocation and deallocation
*/

func NewBitmapFreeList(bitmap []byte, start, end uint64) FreeList {
	size := len(bitmap) * 8
	next := make([]int, size)
	head := -1 // Start with no free pages
	prev := -1 // Track the previous free page

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
			prev = i // Update the previous free page
		} else {
			next[i] = -1 // Mark as allocated
		}
	}

	// Ensure the last free page in the range points to -1
	if prev != -1 {
		next[prev] = -1 // Terminate the linked list
	}

	return &BitmapFreeList{
		bitmap:        bitmap,
		next:          next,
		head:          head,
		modifiedRange: [2]uint64{end, start}, // Initialize with the specified range
		start:         start,
		end:           end,
	}
}
