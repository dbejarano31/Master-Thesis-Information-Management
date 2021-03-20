# Master-Thesis-Information-Management

## Update
We are now working on setting up a Longformer model for the SEC filing classification task. This because of BERT's limitation when it comes to sequences with a length > 512 tokens. In Longformer3_0.ipynb you can find the code, but our Colab GPU runs out of memory because of the size of the model. I read [here] (https://github.com/allenai/longformer/issues/41) that using fp16 could help me save memory, but I haven't figured out where exactly to set it up in my code.
We are also working on the exploratory analysis of the stock prices to determine a more robust way for identifying price deviations.


## File notes
The data used for the models in the repo is [here] (https://raw.githubusercontent.com/dbejarano31/Master-Thesis-Information-Management/main/consolidated-data).

