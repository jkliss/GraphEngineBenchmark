cat example-undirected.e | awk '{print; print $2" "$1" "$3}' | sort -n --parallel 2 -S 70%
