#!/bin/bash
input=$1
output=$2
mkdir -p "$output"

find "$input" -name '*.gz' | while IFS=$'\n' read -r FILE; do
  filename=$(basename "$FILE")
  gunzip -d -c "$FILE" > "$output$filename"_unpack
done
find "$input" -name "*.[^(gz)]" | while IFS=$'\n' read -r FILE; do
  cp "$FILE" "$output"
done
