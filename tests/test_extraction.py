from app.modules.Extraction import Postgresql_fecth_excecutor,download_csv_from_gcs,api_extracion


def test_Postgresql_fecth_excecutor():
    postgresql = Postgresql_fecth_excecutor()
    assert postgresql.db_host == "35.239.114.35"
    assert postgresql.db_name == "test-data-mining"
    assert postgresql.db_user == "test-data-mining"
    assert postgresql.db_password == "test-data-mining-lax-2022"

def test_download_csv_from_gcs():
    df = download_csv_from_gcs('customer_experience_bucket', 'mineria_venta.csv', 'customer experience')
    assert not df.empty

def test_api_extraction():
    url="https://v1-api.lax.marketing/api/blacklist/downloadFile"
    authorization='Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZFVzZXIiOjg5LCJpZENvbXBhbnkiOjQzLCJyb2xVc2VyIjoyLCJpYXQiOjE2ODIwODM2NDN9.HvKEkVpeSmC3xnfq9HFzpT2usSewnTfBDkOVNMQ2k9Q'
    df = api_extracion(url, authorization=authorization)
    assert not df.empty