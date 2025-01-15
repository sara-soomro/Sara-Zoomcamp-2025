terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "ivory-period-444518-v9"
  region  = "us-central1"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "sarabucket-ivory-period-444518-v9"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 60 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "bigquerydataset" {
  dataset_id = "big_query_ds_ivory_period_444518_v9"
  project    = "ivory-period-444518-v9"
  location   = "US"
}
