import json
import datasets

# def process_image(filenames, image_folder, condition_folder):
#     from modules.models.hed import HedDetector
#     hed_model = HedDetector()
#     for file in filenames:
#         file_path = os.path.join(image_folder, file)
#         save_file_path = os.path.join(condition_folder, file)
#         img = np.array(Image.open(file_path).convert("RGB"))
#         edge_image = hed_model(img)
#         edge_img = Image.fromarray(edge_image)
#         edge_img.save(save_file_path)


# regexp = r'/256'
# regexp = re.compile(regexp)
#
#
# for key in data.keys():
#     image_url = data[key]["imgSrc"]
#     if regexp.search(image_url):
#         image_url = re.sub(regexp, "/public", image_url)

# image_path.replace("")


# image_folder = "element"

# downloaded_files = {
#     "train": "a.txt",
#     "dev": "b.txt"
# }

# url = "http://images.cocodataset.org/val2017/000000039769.jpg"
# image = Image.open(requests.get(url, stream=True).raw)
#
# test = {"a": [1, 2, 3], "image": [image, image, image]}

# class ElementDataset(Dataset):
#     """
#     A dataset to prepare the instance and class images with the prompts for fine-tuning the model.
#     It pre-processes the images and the tokenizes prompts.
#     """
#     def __init__(
#         self,
#         instance_data_root,
#         tokenizer,
#         class_data_root=None,
#         class_prompt=None,
#         size=512,
#         center_crop=False,
#     ):
#         self.size = size
#         self.center_crop = center_crop
#         self.tokenizer = tokenizer
#
#         self.instance_data_root = Path(instance_data_root)
#         if not self.instance_data_root.exists():
#             raise ValueError("Instance images root doesn't exists.")
#
#         self.instance_images_path = list(Path(instance_data_root).iterdir())
#         self.num_instance_images = len(self.instance_images_path)
#         self.instance_prompt = instance_prompt
#         self._length = self.num_instance_images
#
#         if class_data_root is not None:
#             self.class_data_root = Path(class_data_root)
#             self.class_data_root.mkdir(parents=True, exist_ok=True)
#             self.class_images_path = list(self.class_data_root.iterdir())
#             self.num_class_images = len(self.class_images_path)
#             self._length = max(self.num_class_images, self.num_instance_images)
#             self.class_prompt = class_prompt
#         else:
#             self.class_data_root = None
#
#         self.image_transforms_resize_and_crop = transforms.Compose(
#             [
#                 transforms.Resize(size, interpolation=transforms.InterpolationMode.BILINEAR),
#                 transforms.CenterCrop(size) if center_crop else transforms.RandomCrop(size),
#             ]
#         )
#
#         self.image_transforms = transforms.Compose(
#             [
#                 transforms.ToTensor(),
#                 transforms.Normalize([0.5], [0.5]),
#             ]
#         )
#
#     def __len__(self):
#         return self._length
#
#     def __getitem__(self, index):
#         example = {}
#         instance_image = Image.open(self.instance_images_path[index % self.num_instance_images])
#         if not instance_image.mode == "RGB":
#             instance_image = instance_image.convert("RGB")
#         instance_image = self.image_transforms_resize_and_crop(instance_image)
#
#         example["PIL_images"] = instance_image
#         example["instance_images"] = self.image_transforms(instance_image)
#
#         example["conditioning_pixel_values"] = conditioning_images
#
#         example["instance_prompt_ids"] = self.tokenizer(
#             self.instance_prompt,
#             padding="do_not_pad",
#             truncation=True,
#             max_length=self.tokenizer.model_max_length,
#         ).input_ids
#
#         if self.class_data_root:
#             class_image = Image.open(self.class_images_path[index % self.num_class_images])
#             if not class_image.mode == "RGB":
#                 class_image = class_image.convert("RGB")
#             class_image = self.image_transforms_resize_and_crop(class_image)
#             example["class_images"] = self.image_transforms(class_image)
#             example["class_PIL_images"] = class_image
#             example["class_prompt_ids"] = self.tokenizer(
#                 self.class_prompt,
#                 padding="do_not_pad",
#                 truncation=True,
#                 max_length=self.tokenizer.model_max_length,
#             ).input_ids
#
#         return example


# def generate_data():
#     image_folder = "element"
#     condition_folder = os.path.join(image_folder, "condition")
#     file_names = [filename for filename in os.listdir(image_folder) if
#                   filename.lower().endswith(('.png', '.jpg', '.jpeg'))]
#     if not os.path.exists(condition_folder):
#         os.mkdir(condition_folder)
#         process_image(file_names, image_folder, condition_folder)
#     for index in range(len(file_names)):
#         filename = file_names[index]
#         caption_filename = filename.replace('.jpg', '.txt') \
#             .replace(".png", ".txt")
#         conditional_file = os.path.join(condition_folder, os.path.basename(filename))
#
#         caption_file = os.path.join(image_folder, caption_filename)
#         image_file = os.path.join(image_folder, filename)
#         with open(caption_file) as f:
#             caption = f.read()
#         yield {
#             "image": Image.open(image_file),
#             "conditioning_image": Image.open(conditional_file),
#             "text": caption
#         }
#

# def generate_template(image_folder):
#     import pandas as pd
#     condition_folder = os.path.join(image_folder, "condition")
#     mask_folder = os.path.join(image_folder, "mask")
#     file_names = [filename for filename in os.listdir(image_folder) if
#                   filename.lower().endswith(('.png', '.jpg', '.jpeg'))]
#     prompt_file = os.path.join(image_folder, "prompt.csv")
#     prompt_data = pd.read_csv(prompt_file)
#     prompt_dict = {}
#     for values in prompt_data.values:
#         image_name = values[0].split("/")[-2] + ".png"
#         prompt_dict[image_name] = values[1]
#     # if not os.path.exists(condition_folder):
#     #     os.mkdir(condition_folder)
#     #     process_image(file_names, image_folder, condition_folder)
#     results = []
#     for index in range(len(file_names)):
#         filename = file_names[index]
#         prompt = prompt_dict[filename]
#         conditional_file = os.path.join(condition_folder, os.path.basename(filename))
#         image_file = os.path.join(image_folder, filename)
#         mask_file = os.path.join(mask_folder, filename)
#         results.append({
#             "image": Image.open(image_file),
#             "conditioning_image": Image.open(conditional_file),
#             "mask": Image.open(mask_file),
#             "text": prompt
#         })
#
#     return results


def generate_templates():
    file_path = "items.json"
    # 打开JSON文件
    with open(file_path, 'r') as f:
        data = json.load(f)
        for item in data:
            yield item

# class element_data_set(datasets.GeneratorBasedBuilder):
#     # def __init__(self, **kwargs):
#     #     super().__init__(**kwargs)
#     #     self.condition_folder = os.path.join(image_folder, "condition")
#     #     self.file_names = [filename for filename in os.listdir(image_folder)
#     #                        if filename.lower().endswith(('.png', '.jpg', '.jpeg'))]
#     #     if not os.path.exists(self.condition_folder):
#     #         os.mkdir(self.condition_folder)
#     #         process_image(self.file_names, image_folder, self.condition_folder)
#
#     def _generate_examples(self, filepath):
#         pass
#         # results = generate_templates()
#         # for item in results:
#         #     yield item
#
#     def _info(self) -> DatasetInfo:
#         return datasets.DatasetInfo(
#             # description="train",
#             features=datasets.Features(
#                 {
#                     "question_id": datasets.Value(dtype="int16"),
#                     "title": datasets.Value(dtype='string'),
#                     "context": datasets.Value(dtype='string'),
#                     "score": datasets.Value(dtype='int16'),
#                     "answer": datasets.Value(dtype='string')
#                 }
#             )
#         )
#
#     def _split_generators(self, dl_manager: DownloadManager):
#         # urls_to_download = self._URLS
#         # downloaded_files = dl_manager.download_and_extract(urls_to_download)
#
#         return [
#             datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={"filepath": "element"})
#             # datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={"filepath": downloaded_files["dev"]}),
#         ]


xinLin_data_set = datasets.Dataset.from_generator(generate_templates, features=datasets.Features(
    {
        "question_id": datasets.Value(dtype="string"),
        "title": datasets.Value(dtype='string'),
        "context": datasets.Value(dtype='string'),
        "score": datasets.Value(dtype='int16'),
        "answer": datasets.Value(dtype='string')
    }
))

xinLin_data_set.push_to_hub("Superlang/xinLin_data_set")
xinLin_data_set.save_to_disk("./xinLin_data_set")

# datasets.Dataset.from_list()

# datasets = element_data_set()
# data = datasets.load_from_disk("./xinLin_data_set")
# print(data[0])