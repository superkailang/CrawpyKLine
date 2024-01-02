from PIL import Image
import requests
from datasets import Dataset, Features, Value
import datasets
import pandas as pd

# datasets.load_dataset()

# we need to define the features ourselves
features = Features({
    'a': Value(dtype='int32'),
    'b': datasets.Image(decode=True),
    "image": datasets.Image(decode=True, id=None)
})

url = "http://images.cocodataset.org/val2017/000000039769.jpg"
image = Image.open(requests.get(url, stream=True).raw)

# df = pd.DataFrame({"a": [1, 2, 3], "b": [image, image, image]})

test = {"a": [1, 2, 3], "b": [image, image, image],
        "image": [url, url, url]}

# dataset = Dataset.from_pandas(df, features=features)
# assuming you're logged in with your token

# datasets.load_dataset()

dataset = Dataset.from_dict({"image": ["image/image_0mx.png"]}).cast_column("image", datasets.Image())


# dataset = Dataset.from_dict(test).cast_column("image", datasets.Image())

dataset.push_to_hub("bsc")
