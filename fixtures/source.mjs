export const seed={
  "runId": "run1",
  "job": {
    "namespace": "local",
    "name": "prepare"
  },
  "outputs": [
    {
      "namespace": "local",
      "name": "clean"
    }
  ]
};
export const seedPath="/run-summary";
export const routes=[
  {
    "path": "/events",
    "body": {
      "next": "/events?offset=1",
      "events": [
        {
          "eventId": "e2",
          "eventTime": 20,
          "eventType": "COMPLETE",
          "run": {
            "runId": "run1"
          },
          "job": {
            "namespace": "local",
            "name": "prepare"
          },
          "outputs": [
            {
              "namespace": "local",
              "name": "clean",
              "outputFacets": {
                "rows": {
                  "producer": "p1",
                  "schemaURL": "urn:rows:v1",
                  "count": 12
                }
              }
            }
          ]
        }
      ]
    },
    "method": "GET",
    "status": 200,
    "query": {
      "offset": "0"
    },
    "headers": {}
  },
  {
    "path": "/events",
    "body": {
      "next": null,
      "events": [
        {
          "eventId": "e1",
          "eventTime": 10,
          "eventType": "START",
          "run": {
            "runId": "run1"
          },
          "job": {
            "namespace": "local",
            "name": "prepare"
          },
          "inputs": [
            {
              "namespace": "local",
              "name": "raw",
              "facets": {
                "schema": {
                  "producer": "p1",
                  "schemaURL": "urn:schema:v1",
                  "fields": [
                    "id"
                  ]
                }
              }
            }
          ]
        }
      ]
    },
    "method": "GET",
    "status": 200,
    "query": {
      "offset": "1"
    },
    "headers": {}
  }
];
export const phases={
  "rerun": [
    {
      "path": "/events",
      "body": {
        "next": null,
        "events": [
          {
            "eventId": "e3",
            "eventTime": 30,
            "eventType": "FAIL",
            "run": {
              "runId": "run2"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "outputs": [
              {
                "namespace": "local",
                "name": "clean",
                "outputFacets": {
                  "rows": {
                    "producer": "p1",
                    "schemaURL": "urn:rows:v1",
                    "count": 3
                  }
                }
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    }
  ],
  "schema-conflict": [
    {
      "path": "/events",
      "body": {
        "next": "/events?offset=1",
        "events": [
          {
            "eventId": "conflict1",
            "eventTime": 20,
            "eventType": "COMPLETE",
            "run": {
              "runId": "run1"
            },
            "job": {
              "namespace": "local",
              "name": "prepare"
            },
            "outputs": [
              {
                "namespace": "local",
                "name": "clean",
                "outputFacets": {
                  "rows": {
                    "producer": "p1",
                    "schemaURL": "urn:rows:v2",
                    "count": 12
                  }
                }
              }
            ]
          }
        ]
      },
      "method": "GET",
      "status": 200,
      "query": {
        "offset": "0"
      },
      "headers": {}
    }
  ]
};
export const portalKind="";
