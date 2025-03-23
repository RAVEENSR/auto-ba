import os

import pandas as pd

REPO_NAME = os.getenv("REPO_NAME")
if not REPO_NAME:
    raise ValueError("REPO_NAME is not set. Please set the REPO_NAME environment variable.")

# === Helper Functions ===
def get_related_prs(pr_df, issue_id):
    """Find PRs that reference the issue ID."""
    return pr_df[pr_df['prDescription'].str.contains(f"#{issue_id}", na=False)]

def get_file_paths(prs):
    """Combine file paths from all related PRs."""
    all_paths = []
    for paths in prs['prFilePaths'].dropna():
        all_paths.extend(paths.split("; "))
    return "; ".join(all_paths)

def get_resolvers(prs, commenters, closed_by):
    """Determine resolvers based on PR creators, commenters, or closedBy."""
    resolvers = "; ".join(prs['prCreatorLoginId'].dropna().unique())
    if resolvers:
        return resolvers
    elif pd.notna(commenters):
        return commenters
    else:
        return closed_by



def main():
    # === Load Data ===
    issue_df = pd.read_csv(f"{REPO_NAME}_issue_details.csv")
    pr_df = pd.read_csv(f"{REPO_NAME}_related_pr_details.csv")

    # === Apply Logic to Issues ===
    issue_df['files'] = issue_df['issueId'].apply(lambda x: get_file_paths(get_related_prs(pr_df, x)))
    issue_df['resolvers'] = issue_df.apply(
        lambda row: get_resolvers(get_related_prs(pr_df, row['issueId']), row['commenters'], row['closedBy']),
        axis=1
    )

    # === Rename Columns ===
    new_column_names = [
        "issue_id", "creator_login_id", "created_date", "closed_date",
        "closed_by", "commenters", "title", "description", "files", "resolvers"
    ]

    issue_df.columns = new_column_names

    # === Save Updated File ===
    output_file = f"{REPO_NAME}_updated_issue_details.csv"
    issue_df.to_csv(output_file, index=False)

    print(f"✅ File saved as '{output_file}'")


if __name__ == "__main__":
    main()
