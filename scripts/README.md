# To parquet or not to parquet? 

As a general rule of thumb, for training data around 1 million datapoints, a Parquet dataset is sufficient. However, as the dataset size increases (e.g., 5 million or more), switching to a WebDataset can provide up to a 5x speed improvement during training.

We envision OpenBioScience to be the foundation for training the next generation of both specialized (e.g., pathology, radiology) and general biomedical models. To train specialized models, it's necessary to filter OpenBioScience, which is a task well-suited for Parquet datasets.

Therefore, we offer our dataset in both formats:

* WebDataset – optimized for large-scale model training, especially when streaming data.
* Parquet Dataset – ideal for performing fast, structured filtering and querying of the dataset.
  
Both formats support streaming!

If you're interested in the tradeoffs between Parquet and WebDataset, please read our last section on Serialization.



# How do we serialzie OpenBioSciecne? 





# Serialization

**WebDataset** and **Parquet Dataset** are two different formats used for storing and handling large datasets. Each has its own advantages and use cases. Let's break down their differences:

### 1. **Data Storage and Format**
   - **WebDataset**: This format stores data as sequences of files (e.g., `.tar` archives or individual files). Each file contains a data sample, which can be an image, label, or any other modality (text, img, etc.). These files are often stored in `.txt`, `.jpeg` 
 respectively. 
   
   - **Parquet Dataset**: This is a columnar storage format designed for **high-performance analytical queries**. Parquet compresses and encodes the data efficiently. 

### 2. **Use Cases**

   - **WebDataset**: Ideal for large-scale image datasets or multimodal data stored as individual files. It's particularly useful for **streaming** data (e.g., during training) without loading the entire dataset into memory. This makes it effective for distributed training scenarios, where data is read dynamically from remote locations.

   - **Parquet Dataset**: Primarily used for storing structured, relational, or tabular data. Parquet is efficient when running **analytical queries** over large datasets due to its columnar layout. It excels at compression and fast retrieval, making it suitable for databases or batch processing.



### 3. **Scalability**
   - **WebDataset**: Scales well in distributed and cloud environments since data can be stored in object storage (e.g., S3, Google Cloud Storage) and streamed without needing to download the entire dataset. It’s designed to handle large, unstructured datasets (e.g., images, audio).
   
   - **Parquet Dataset**: Scales well for structured data and is highly performant in big data frameworks. It works well for datasets that need fast columnar access and are analyzed using database-like operations.

### Summary Table:

| Aspect                  | WebDataset                                | Parquet Dataset                           |
|-------------------------|-------------------------------------------|-------------------------------------------|
| **Format Type**          | File-based (e.g., `.tar`, `.jpg`, `.png`) | Columnar (e.g., `.parquet`)               |
| **Best Use Case**        | Image/audio/text streaming                | Tabular, structured data                  |
| **Data Access**          | Streaming, file-by-file                   | Columnar, selective column retrieval      |
| **Performance**          | Efficient for I/O-heavy, unstructured data| Efficient for batch processing, analytics |
| **Integration**          | PyTorch, TensorFlow, ML pipelines         | Spark, Hadoop, pandas, Arrow              |
| **File Size**            | Many small files or archives              | Large, highly compressed files            |
| **Scalability**          | Good for distributed/cloud environments   | Good for structured, big data ecosystems  |

The choice between WebDataset and Parquet depends largely on the type of data you're working with and the specific performance characteristics required by your workflow.

