import csv
import logging
import random

from faker import Faker
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_RECORDS = 5000
OUTPUT_FILE = "loans.csv"
GCS_BUCKET = "credit-risk-raw-data"


def did_default(applicant: dict) -> int:
    risk = 0.05

    if applicant["credit_score"] < 600:
        risk += 0.30
    elif applicant["credit_score"] < 680:
        risk += 0.15

    loan_to_income = applicant["loan_amount"] / applicant["annual_income"]
    if loan_to_income > 0.5:
        risk += 0.25

    if applicant["existing_debt"] > 25_000:
        risk += 0.15

    if applicant["interest_rate"] > 15.0:
        risk += 0.10

    risk = min(risk, 0.95)
    return 1 if random.random() < risk else 0


def generate_applicant(applicant_id: int) -> dict:
    age = random.randint(21, 70)
    annual_income = random.randint(20_000, 200_000)
    employment_years = random.randint(0, 40)
    credit_score = random.randint(300, 850)
    existing_debt = random.randint(0, 50_000)
    loan_amount = random.randint(1_000, 100_000)
    loan_term_months = random.choice([12, 24, 36, 48, 60])
    interest_rate = round(random.uniform(3.0, 25.0), 2)
    home_ownership = random.choice(["RENT", "OWN", "MORTGAGE"])
    loan_purpose = random.choice(
        ["car", "home", "education", "medical", "business", "debt_consolidation"]
    )

    defaulted = did_default({
        "credit_score": credit_score,
        "annual_income": annual_income,
        "loan_amount": loan_amount,
        "existing_debt": existing_debt,
        "interest_rate": interest_rate,
    })

    return {
        "applicant_id": applicant_id,
        "age": age,
        "annual_income": annual_income,
        "employment_years": employment_years,
        "credit_score": credit_score,
        "existing_debt": existing_debt,
        "loan_amount": loan_amount,
        "loan_term_months": loan_term_months,
        "interest_rate": interest_rate,
        "home_ownership": home_ownership,
        "loan_purpose": loan_purpose,
        "defaulted": defaulted,
    }


def generate_dataset(num_records: int) -> list[dict]:
    logger.info("Generating %d loan records...", num_records)
    records = []
    for applicant_id in range(1, num_records + 1):
        records.append(generate_applicant(applicant_id))
    return records


def write_csv(records: list[dict], output_path: str) -> None:
    logger.info("Writing %d records to %s", len(records), output_path)
    fieldnames = records[0].keys()
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str) -> None:
    logger.info("Uploading %s to gs://%s/%s", source_file, bucket_name, destination_blob)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    logger.info("Upload complete.")


def main() -> None:
    records = generate_dataset(NUM_RECORDS)
    write_csv(records, OUTPUT_FILE)
    upload_to_gcs(GCS_BUCKET, OUTPUT_FILE, f"raw/{OUTPUT_FILE}")


if __name__ == "__main__":
    main()
