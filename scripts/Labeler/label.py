"""
Overclustering images and labeling them using a LLM.
"""

import os
import torch
import numpy as np
from pathlib import Path
from tqdm import tqdm
import matplotlib.pyplot as plt
from vllm import LLM, SamplingParams
import umap
from sklearn.cluster import KMeans
import yaml
import json
from typing import List, Dict, Tuple
import pandas as pd

class BiomedicalImageClustering:
    def __init__(self, 
                 data_path: str,
                 output_dir: str,
                 n_clusters: int = 2000,
                 n_dims: int = 25,
                 batch_size: int = 1000,
                 samples_per_shard: int = 1900):
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.n_clusters = n_clusters
        self.n_dims = n_dims
        self.batch_size = batch_size
        self.samples_per_shard = samples_per_shard
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize VLLM
        print("\n=== Initializing LLM ===")
        self.llm = LLM(model="llava-hf/llava-onevision-qwen2-0.5b-ov-hf", 
                       max_model_len=10000)
        
        # Load taxonomy
        self.load_taxonomy()
        
    def load_taxonomy(self):
        """Load and flatten taxonomy into list of leaf labels"""
        with open('src/pmc_oa/taxonomy.yml', 'r') as f:
            taxonomy = yaml.safe_load(f)
            
        self.labels = []
        def extract_leaves(d):
            if isinstance(d, dict):
                for v in d.values():
                    extract_leaves(v)
            elif isinstance(d, list):
                self.labels.extend(d)
                
        extract_leaves(taxonomy)
        print(f"Loaded {len(self.labels)} possible labels")
        
    def load_features(self):
        """Load features and images using existing loader"""
        from pmc_oa.data_loaders.dino_features_loader import load_and_concatenate_subsets
        
        print("\n=== Loading Data ===")
        print(f"Data path: {self.data_path}")
        print(f"Samples per shard: {self.samples_per_shard}")
        
        data = load_and_concatenate_subsets(
            self.data_path,
            samples_per_shard=self.samples_per_shard,
            limit=20
        )
        
        print(f"Loaded features shape: {data['features'].shape}")
        print(f"Loaded images shape: {data['images'].shape}")
        print(f"Number of keys: {len(data['keys'])}")
        
        # Convert images to torch tensor
        data["images"] = torch.from_numpy(data["images"])
        print("Converted images to torch tensor")
        return data
        
    def reduce_dimensions(self, features: np.ndarray) -> np.ndarray:
        """Reduce dimensions using UMAP"""
        print(f"\n=== Reducing Dimensions with UMAP ===")
        print(f"Input feature shape: {features.shape}")
        print(f"Target dimensions: {self.n_dims}")
        
        reducer = umap.UMAP(n_components=self.n_dims, random_state=42)
        reduced = reducer.fit_transform(features)
        
        print(f"Reduced feature shape: {reduced.shape}")
        return reduced
        
    def cluster_features(self, features: np.ndarray) -> np.ndarray:
        """Cluster features using KMeans"""
        print(f"\n=== Clustering Features ===")
        print(f"Input feature shape: {features.shape}")
        print(f"Number of clusters: {self.n_clusters}")
        
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42)
        clusters = kmeans.fit_predict(features)
        
        print(f"Unique clusters: {len(np.unique(clusters))}")
        return clusters
        
    def generate_cluster_summary(self, cluster_idx: int, images: torch.Tensor) -> np.ndarray:
        """Generate summary image for cluster"""
        print(f"\nGenerating summary for cluster {cluster_idx}")
        print(f"Number of images in cluster: {len(images)}")
        
        fig, axes = plt.subplots(nrows=2, ncols=10, figsize=(100, 20))
        axes = axes.flatten()
        
        for i, ax in enumerate(axes):
            if i < len(images):
                ax.imshow(images[i].permute(1, 2, 0))
            ax.axis('off')
            
        plt.tight_layout()
        
        # Convert plot to image array
        fig.canvas.draw()
        summary_image = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)
        summary_image = summary_image.reshape(fig.canvas.get_width_height()[::-1] + (3,))
        
        print(f"Summary image shape: {summary_image.shape}")
        plt.close(fig)
        return summary_image
        
    def get_label_from_llm(self, summary_images: List[np.ndarray]) -> List[str]:
        """Get labels from LLM for batch of summary images"""
        print(f"\n=== Getting Labels from LLM ===")
        print(f"Processing batch of {len(summary_images)} images")
        
        prompt_template = f"""This image shows a grid of 20 related biomedical images. 
        Based on these examples, classify this group into exactly one of the following categories: 
        {', '.join(self.labels)}. 
        Choose the single most appropriate category. 
        Response format: Just return the category name, nothing else."""
        
        sampling_params = SamplingParams(temperature=0.1, max_tokens=50)
        prompts = [prompt_template] * len(summary_images)
        
        outputs = self.llm.generate(prompts, sampling_params)
        labels = [output.outputs[0].text.strip() for output in outputs]
        
        print("Generated labels:", labels)
        return labels
        
    def save_results(self, 
                    cluster_idx: int, 
                    summary_image: np.ndarray, 
                    label: str, 
                    images: torch.Tensor,
                    image_keys: List[str]):
        """Save results for a cluster"""
        print(f"\n=== Saving Results for Cluster {cluster_idx} ===")
        print(f"Label: {label}")
        print(f"Number of images: {len(images)}")
        
        cluster_dir = self.output_dir / f"cluster_{cluster_idx:04d}"
        cluster_dir.mkdir(parents=True, exist_ok=True)
        
        # Save summary image
        summary_path = cluster_dir / "summary.png"
        plt.imsave(summary_path, summary_image)
        print(f"Saved summary image to {summary_path}")
        
        # Save individual images
        images_dir = cluster_dir / "images"
        images_dir.mkdir(exist_ok=True)
        print(f"Saving {len(images)} individual images...")
        
        for i, (image, key) in enumerate(zip(images, image_keys)):
            image_path = images_dir / f"image_{i:04d}.png"
            plt.imsave(image_path, image.permute(1, 2, 0).numpy())
        
        # Save metadata
        metadata = {
            "cluster_id": cluster_idx,
            "label": label,
            "image_keys": image_keys
        }
        metadata_path = cluster_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"Saved metadata to {metadata_path}")
        
    def process(self):
        """Main processing pipeline"""
        print("\n=== Starting Processing Pipeline ===")
        print(f"Output directory: {self.output_dir}")
        
        # Load and process data
        data = self.load_features()
        reduced_features = self.reduce_dimensions(data["features"])
        clusters = self.cluster_features(reduced_features)
        
        # Process clusters in batches
        unique_clusters = np.unique(clusters)
        results = []
        
        print(f"\n=== Processing {len(unique_clusters)} Clusters in Batches ===")
        print(f"Batch size: {self.batch_size}")
        
        for i in tqdm(range(0, len(unique_clusters), self.batch_size)):
            print(f"\nProcessing batch {i//self.batch_size + 1}")
            batch_clusters = unique_clusters[i:i + self.batch_size]
            batch_summaries = []
            batch_images = []
            batch_keys = []
            
            # Generate summaries for batch
            for cluster_idx in batch_clusters:
                cluster_mask = clusters == cluster_idx
                cluster_images = data["images"][cluster_mask][:20]  # Take up to 20 images
                cluster_keys = data["keys"][cluster_mask][:20]
                
                summary = self.generate_cluster_summary(cluster_idx, cluster_images)
                batch_summaries.append(summary)
                batch_images.append(cluster_images)
                batch_keys.append(cluster_keys)
            
            # Get labels for batch
            batch_labels = self.get_label_from_llm(batch_summaries)
            
            # Save results for batch
            for cluster_idx, summary, label, images, keys in zip(
                batch_clusters, batch_summaries, batch_labels, batch_images, batch_keys):
                self.save_results(cluster_idx, summary, label, images, keys)
                results.append({
                    "cluster_id": cluster_idx,
                    "label": label,
                    "n_images": len(images)
                })
                
        # Save overall results
        results_path = self.output_dir / "cluster_labels.csv"
        df = pd.DataFrame(results)
        df.to_csv(results_path, index=False)
        print(f"\nSaved cluster labels to {results_path}")
        
        print("\nProcessing complete!")
        
def main():
    clustering = BiomedicalImageClustering(
        data_path="/pasteur/u/ale9806/Repositories/pmc-oa/DinoFeatures",
        output_dir="clustering_results",
        n_clusters=2000,
        n_dims=25,
        batch_size=16  # Process 16 clusters at a time
    )
    clustering.process()

if __name__ == "__main__":
    main()