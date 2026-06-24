import csv 
import random 
from faker import Faker 

fake = Faker() 
Faker.seed(42) 
random.seed(42) 

NUM_RECORDS = 5000

def generate_applicant(applicant_id: int) -> dict: 

age = random.randint(21, 70)
annual_income = random.randint(20_000, 200_000)
employment_years = random.randint(0, 40)

credit_score = random.randint(300,850) 
existing_debt = random.randint(0,50_000) 
loan_amount = random.randint(1_000, 100_000) 
loan_term_months = random.choice([12, 24, 36, 48, 60]) 
interest_rate = round(random.uniform(3.0,25.0),2) 
home_ownership = random.choice(["RENT", "OWN", "MORTGAGE"]) 
loan_purpose = random.choice([ "car","home","education","medical","business","debt_consolidation"]) 

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
}
