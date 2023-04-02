terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google-beta" {
  project     = var.project
  region      = var.region
  zone        = var.zone
}


# DWH: BigQuery
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

# Airflow: Google Cloud Composer
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment

resource "google_project_service" "composer_api" {
  provider = google-beta
  project = var.project
  service = "composer.googleapis.com"
  // Disabling Cloud Composer API might irreversibly break all other
  // environments in your project.
  disable_on_destroy = false
}

resource "google_composer_environment" "cloud-composer" {
  provider = google-beta
  name = "crime-de-capstone"
  region = "us-central1"

  config {
    software_config {
      image_version = "composer-2.1.11-airflow-2.4.3"
    }
    node_config {
      service_account = "sa-capstone-crime@de-training-381317.iam.gserviceaccount.com"
    }
  }
}


