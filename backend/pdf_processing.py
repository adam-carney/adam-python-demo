from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from dotenv import load_dotenv
import os
load_dotenv()
endpoint = os.getenv("ASQ3_PDF_API_ENDPOINT")
key = os.getenv("ASQ3_PDF_API_KEY")
model_id = os.getenv("CUSTOM_BUILT_MODEL_ID")

document_analysis_client = DocumentAnalysisClient(
    endpoint=endpoint, credential=AzureKeyCredential(key)
)
path_to_sample_documents = "/Users/adamcarney/Documents/Scanned Documents/JamesWright_09292924.pdf"
# Make sure your document's type is included in the list of document types the custom model can analyze
with open(path_to_sample_documents, "rb") as f:
    poller = document_analysis_client.begin_analyze_document(
        model_id=model_id, document=f
    )
result = poller.result()

for idx, document in enumerate(result.documents):
    print(f"--------Analyzing document #{idx + 1}--------")
    print(f"Document has type {document.doc_type}")
    print(f"Document has document type confidence {document.confidence}")
    print(f"Document was analyzed with model with ID {result.model_id}")
    for name, field in document.fields.items():
        field_value = field.value if field.value else field.content
        print(
            f"......found field of type '{name}' with value '{field_value}' and with confidence {field.confidence}"
        )
"""
# iterate over tables, lines, and selection marks on each page
for page in result.pages:
    print(f"\nLines found on page {page.page_number}")
    for line in page.lines:
        print(f"...Line '{line.content}'")
    for word in page.words:
        print(f"...Word '{word.content}' has a confidence of {word.confidence}")
    if page.selection_marks:
        print(f"\nSelection marks found on page {page.page_number}")
        for selection_mark in page.selection_marks:
            print(
                f"...Selection mark is '{selection_mark.state}' and has a confidence of {selection_mark.confidence}"
            )

for i, table in enumerate(result.tables):
    print(f"\nTable {i + 1} can be found on page:")
    for region in table.bounding_regions:
        print(f"...{region.page_number}")
    for cell in table.cells:
        print(
            f"...Cell[{cell.row_index}][{cell.column_index}] has text '{cell.content}'"
        )
print("-----------------------------------")

"""