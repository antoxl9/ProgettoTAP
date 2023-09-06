#!/bin/bash

set -e

# Create the Neo4j database
neo4j-admin create --database=astroKg --force

# Start the Neo4j server
neo4j start
