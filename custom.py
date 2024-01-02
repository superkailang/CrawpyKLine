import datasets
from datasets import DownloadManager, DatasetInfo
from PIL import Image
import requests

downloaded_files = {
    "train": "a.txt",
    "dev": "b.txt"
}

url = "http://images.cocodataset.org/val2017/000000039769.jpg"
image = Image.open(requests.get(url, stream=True).raw)

test = {"a": [1, 2, 3], "image": [image, image, image]}


class BSC(datasets.GeneratorBasedBuilder):
    def _generate_examples(self, **kwargs):
        for index in range(len(test)):
            yield index, {
                "image": test["image"][index],
                "label": test["a"][index],
            }

    def _info(self) -> DatasetInfo:
        return datasets.DatasetInfo(
            description="test",
            features=datasets.Features(
                {
                    "image": datasets.Image(),
                    "label": datasets.Value(dtype='int32')
                }
            )
        )

    def _split_generators(self, dl_manager: DownloadManager):
        # urls_to_download = self._URLS
        # downloaded_files = dl_manager.download_and_extract(urls_to_download)

        return [
            datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={"filepath": downloaded_files["train"]}),
            datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={"filepath": downloaded_files["dev"]}),
        ]
