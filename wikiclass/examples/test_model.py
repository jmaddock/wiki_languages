import pickle
import sys; sys.path.insert(0, ".")
import csv; csv.field_size_limit(sys.maxsize)

from wikiclass import assessments, languages
from wikiclass.models import RFTextModel
from wikiclass.features import WikitextAndInfonoise

# Train and test set ("<assessment class>", "text content")
input_file = open("datasets/assessed_revisions.with_text.tsv")
train_set = []
test_set = []
for row in csv.DictReader(input_file, delimiter="\t"):
    if row['class'] == "A": continue
    if row['is_test'] == "TRUE":
        test_set.append((row['text'], row['class']))

model = RFTextModel.from_file(open("enwiki.rf_text.model", "rb"))

results = model.test(test_set)

print(results)
