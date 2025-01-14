

---

## How to Train a CLIP-like Model with OpenBioScience

This post builds upon an original discussion on OpenCLIP's [GitHub](https://github.com/mlfoundations/open_clip/discussions/812).

### Introduction

OpenCLIP has gained recognition in both academic and industrial communities as an exceptional open-source framework for training CLIP-like models. However, the documentation can be lacking when it comes to fine-tuning these models for specific downstream tasks using custom datasets. For beginners, this can be overwhelming as they might not know where to begin. This guide outlines some key considerations and best practices for using OpenCLIP effectively.


### Step 1: Create a Virtual Environment

To begin, we need to set up a virtual environment. Based on my own testing, **Python 3.9** works well. You can create the environment using the following command:


```python
# Create env
conda create --name train_clip python=3.9

# Activate env
conda activate train_clip
```
---

### Step 2: Install environment
Check your CUDA version before installing torch and the corresponding packages， if we install the dependencies by directly using official command, we are very likely to encounter a series of errors caused by mismatched torch versions and CUDA versions. So install your environment according to the actual situation.

```python
# Check CUDA versaion
nvidia-smi
```

and we will get the driver version（Using my local device as an example):

```
NVIDIA-SMI 515.65.01 Driver Version: 515.65.01 CUDA Version: 11.7
```

Then visit torch official website to get a compatible  distribution. It is recommended to use  **pip** for installation. For example, for my version **CUDA 11.7** I used:



```python
pip install torch==2.0.1 torchvision==0.15.2 torchaudio==2.0.2 --index-url https://download.pytorch.org/whl/cu117
```

Lastly, verify that the installation was successful:
```python
import torch
print(torch.cuda.is_available())

#  True
```
---
### Step 3: Clone and install the open_clip 

```bash
# Clone repo
git clone https://github.com/mlfoundations/open_clip.git

# Enter the project root directory.
cd open_clip

# Install training dependcies 
pip install -r requirements-training.txt

# Install webdataset
git clone https://github.com/minwoosun/webdataset.git
cd webdataset
git checkout hf-token
pip install -e .

# Install wandb
pip install wandb

# Setup tokens
huggingface-cli login
wandb login
```

---
###  Step 4: Chose a suitable pre-trained model
OpenClip official provides quite a lot pre-trained models of the CLIP series for downloading and usage. You can use the following command to view the specific details of these models.

The first column represents the model’s name, which is also the parameter for text encoding in the model. The second column indicates either the provider of the model or the scale of training dataset used.


```python
import open_clip
open_clip.list_pretrained()

# [('RN50', 'openai'),
#  ('RN50', 'yfcc15m'),
#  ('RN50', 'cc12m'),
#   ...,
#  ('nllb-clip-large-siglip', 'v1')]
```



## 5. Train your model


```python
# Enter the src folder of the open_clip repository
cd open_clip/src

# Create a bash file
vim train_openclip.sh

## Add the following:

# specify which GPUs you want to use.
export CUDA_VISIBLE_DEVICES=0,1,2,3,4,5

# set the training args, Example:
torchrun --nproc_per_node 6 -m training.main \
    --batch-size 500 \
    --precision amp \
    --workers 4 \
    --report-to tensorboard \
    --save-frequency 1 \
    --logs="/path/to/your/local/logs" \
    --dataset-type csv \
    --csv-separator="," \
    --train-data /path/to/your/local/training_dict.csv \
    --csv-img-key filepath \
    --csv-caption-key caption \
    --warmup 1000 \
    --lr=5e-6 \
    --wd=0.1 \
    --epochs=32 \
    --model ViT-B-32 \
    --pretrained /path/to/your/local/model

```


For more detailed args explanation, please refer to：[https://github.com/mlfoundations/open_clip/blob/main/src/training/params.py](https://github.com/mlfoundations/open_clip/blob/main/src/open_clip_train/params.py)




