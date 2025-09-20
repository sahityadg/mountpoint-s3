#!/bin/bash
set -e

# Reset to base commit
git reset --hard bc162b82

# List of commits to cherry-pick and format
commits=(
    "350445aa"  # Switch to real-time recording for OTLP metrics
    "5589ba7e"  # Add spec_unstable_metrics_views feature for exponential histograms  
    "1b86597b"  # Add optional exponential histogram support
    "d20351f4"  # Use Delta temporality by default for OTLP metrics export
    "94c92ad3"  # Add benchmarks option test default and exponential histograms
)

for commit in "${commits[@]}"; do
    echo "Processing commit $commit..."
    
    # Cherry-pick the commit
    git cherry-pick "$commit"
    
    # Run formatting and linting
    echo "Running make fmt..."
    make fmt || true
    
    echo "Running make clippy..."
    make clippy || true
    
    # Add any formatting changes and amend
    git add -A
    git commit --amend --no-edit
    
    echo "Completed commit $commit"
    echo "---"
done

echo "All commits processed with formatting!"
