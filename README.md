##  fast-skiplist

Original repo -> github.com/sean-public/fast-skiplist

### Purpose

Changes have been made to this fork to support storing a range of sequences and a timestamp as the key. This is very 
specific to Sync Gateway's use case and probably won't be generally fit to use for other use cases.

Sync Gateway needs to cache documents over the DCP feed in sequence order, so Sync Gateway needs a way to 
store sequences that don't arrive in the order expected until they eventually arrive over the feed. A skip-list 
with support for storing sequence ranges is a good data structure to do so.

