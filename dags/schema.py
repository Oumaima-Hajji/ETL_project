schema = [
    {
      "fields": [
        {
          "mode": "NULLABLE",
          "name": "page",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "results",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "version",
          "type": "FLOAT"
        },
        {
          "mode": "NULLABLE",
          "name": "seed",
          "type": "STRING"
        }
      ],
      "mode": "NULLABLE",
      "name": "info",
      "type": "RECORD"
    },
    {
      "fields": [
        {
          "mode": "NULLABLE",
          "name": "nat",
          "type": "STRING"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "value",
              "type": "INTEGER"
            },
            {
              "mode": "NULLABLE",
              "name": "name",
              "type": "STRING"
            }
          ],
          "mode": "NULLABLE",
          "name": "id",
          "type": "RECORD"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "sha1",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "salt",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "username",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "md5",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "password",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "sha256",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "uuid",
              "type": "STRING"
            }
          ],
          "mode": "NULLABLE",
          "name": "login",
          "type": "RECORD"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "medium",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "thumbnail",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "large",
              "type": "STRING"
            }
          ],
          "mode": "NULLABLE",
          "name": "picture",
          "type": "RECORD"
        },
        {
          "mode": "NULLABLE",
          "name": "cell",
          "type": "STRING"
        },
        {
          "fields": [
            {
              "fields": [
                {
                  "mode": "NULLABLE",
                  "name": "description",
                  "type": "STRING"
                },
                {
                  "mode": "NULLABLE",
                  "name": "offset",
                  "type": "STRING"
                }
              ],
              "mode": "NULLABLE",
              "name": "timezone",
              "type": "RECORD"
            },
            {
              "mode": "NULLABLE",
              "name": "postcode",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "country",
              "type": "STRING"
            },
            {
              "fields": [
                {
                  "mode": "NULLABLE",
                  "name": "longitude",
                  "type": "FLOAT"
                },
                {
                  "mode": "NULLABLE",
                  "name": "latitude",
                  "type": "FLOAT"
                }
              ],
              "mode": "NULLABLE",
              "name": "coordinates",
              "type": "RECORD"
            },
            {
              "mode": "NULLABLE",
              "name": "state",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "city",
              "type": "STRING"
            },
            {
              "fields": [
                {
                  "mode": "NULLABLE",
                  "name": "name",
                  "type": "STRING"
                },
                {
                  "mode": "NULLABLE",
                  "name": "number",
                  "type": "INTEGER"
                }
              ],
              "mode": "NULLABLE",
              "name": "street",
              "type": "RECORD"
            }
          ],
          "mode": "NULLABLE",
          "name": "location",
          "type": "RECORD"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "age",
              "type": "INTEGER"
            },
            {
              "mode": "NULLABLE",
              "name": "date",
              "type": "TIMESTAMP"
            }
          ],
          "mode": "NULLABLE",
          "name": "dob",
          "type": "RECORD"
        },
        {
          "mode": "NULLABLE",
          "name": "phone",
          "type": "STRING"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "age",
              "type": "INTEGER"
            },
            {
              "mode": "NULLABLE",
              "name": "date",
              "type": "TIMESTAMP"
            }
          ],
          "mode": "NULLABLE",
          "name": "registered",
          "type": "RECORD"
        },
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "last",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "first",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "title",
              "type": "STRING"
            }
          ],
          "mode": "NULLABLE",
          "name": "name",
          "type": "RECORD"
        },
        {
          "mode": "NULLABLE",
          "name": "email",
          "type": "STRING"
        },
        {
          "mode": "NULLABLE",
          "name": "gender",
          "type": "STRING"
        }
      ],
      "mode": "REPEATED",
      "name": "results",
      "type": "RECORD"
    },
    {
      "mode": "NULLABLE",
      "name": "dag_execution_date_time",
      "type": "TIMESTAMP"
    }
  ]