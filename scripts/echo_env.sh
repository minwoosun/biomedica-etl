#!/bin/bash

# Load the .env file
if [ -f .env ]; then
    source .env
fi

# Access the variables
echo "Node: $node"
echo "serialization_slurm_log: $serialization_slurm_log"
