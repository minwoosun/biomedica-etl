import argparse
from huggingface_hub import HfApi

def parse_arguments():
    parser = argparse.ArgumentParser(description="Upload a large folder to the Hugging Face Hub.") 
    parser.add_argument("--repo_type",   type=str,   default="dataset",help="Type of the repository (default: 'dataset').")
    parser.add_argument("--token",       type=str,   required=True,    help="Your Hugging Face API token.")
    parser.add_argument("--repo_id",     type=str,   required=True,    help="Repository ID (e.g., 'username/repo_name').")
    parser.add_argument("--folder_path", type=str,   required=True,    help="Path to the folder you want to upload.")
    parser.add_argument("--num_workers", type=int,   required=True,    help="Number of workers to use for the upload.")
    return parser.parse_args()

def main():
    args = parse_arguments()
    api  = HfApi(token=args.token)
    
    api.upload_large_folder(
        repo_type   = args.repo_type,
        repo_id     = args.repo_id,
        folder_path = args.folder_path,
        num_workers = args.num_workers
    )

if __name__ == "__main__":
    main()
