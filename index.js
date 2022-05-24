"use strict";
const { Pool } = require("pg");
const { uniqBy } = require("lodash");
const format = require("pg-format");
const dotenv = require("dotenv");

dotenv.config();
const args = process.argv.slice(2);

const pool = new Pool({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
});

const axios = require("axios");

async function processPatients(patients) {
  const processedPatient = [];
  for (const bundle of patients) {
    let patientInfo = {
      case_id: bundle.resource.id,
      sex: bundle.resource.gender,
      date_of_birth: bundle.resource.birthDate,
      deceased: bundle.resource.deceasedBoolean,
      date_of_death: bundle.resource.deceasedDateTime || null,
      facility_id: null,
    };

    if (bundle.resource.managingOrganization) {
      patientInfo = {
        ...patientInfo,
        facility_id: String(
          bundle.resource.managingOrganization.reference
        ).split("/")[1],
      };
    }
    if (patientInfo.date_of_birth && patientInfo.date_of_birth.length === 4) {
      patientInfo = {
        ...patientInfo,
        date_of_birth: `${patientInfo.date_of_birth}-01-01`,
      };
    }
    if (
      patientInfo.case_id &&
      patientInfo.date_of_birth &&
      patientInfo.date_of_birth.length === 10 &&
      patientInfo.sex &&
      patientInfo.facility_id
    ) {
      processedPatient.push([
        patientInfo.case_id,
        patientInfo.sex,
        patientInfo.date_of_birth,
        patientInfo.deceased,
        patientInfo.date_of_death,
        patientInfo.facility_id,
      ]);
    }
  }
  return processedPatient;
}

async function insertObs(observations) {
  const obs = [];
  if (observations && observations.length > 0) {
    for (const bundle of observations) {
      const {
        valueQuantity,
        valueCodeableConcept,
        valueString,
        valueBoolean,
        valueInteger,
        valueTime,
        valueDateTime,
        encounter: { reference: ref },
        effectiveDateTime,
        code: {
          coding: [{ display: obs_name, code }],
        },
        subject: { reference },
      } = bundle.resource;

      let realValue =
        valueString ||
        valueBoolean ||
        valueInteger ||
        valueTime ||
        valueDateTime;

      if (valueQuantity) {
        realValue = valueQuantity.value;
      }
      if (valueCodeableConcept) {
        const {
          coding: [{ display }],
        } = valueCodeableConcept;
        realValue = display;
      }
      const patient = String(reference).split("/")[1];
      const encounterId = String(ref).split("/")[1];

      obs.push([
        patient,
        encounterId,
        code,
        obs_name,
        realValue,
        effectiveDateTime,
      ]);
    }
  }
  return obs;
}

async function queryPatients(params) {
  const connection = await pool.connect();
  try {
    const {
      data: { entry: patients, link },
    } = await axios.get("http://smilecdr.mets.or.ug:8000/fhir/Patient", {
      params,
      auth: {
        username: process.env.SMILE_CDR_USERNAME,
        password: process.env.SMILE_CDR_PASSWORD,
      },
    });
    let initialPatients = await processPatients(patients);

    const currentProcessedPatients = uniqBy(initialPatients, (x) => x[0]);
    const query = format(
      `INSERT INTO staging_patient (case_id,sex,date_of_birth,deceased,date_of_death,facility_id) VALUES %L ON CONFLICT (case_id) DO UPDATE 
      SET sex = EXCLUDED.sex,date_of_birth = EXCLUDED.date_of_birth,deceased = EXCLUDED.deceased,date_of_death = EXCLUDED.date_of_death,facility_id = EXCLUDED.facility_id;`,
      currentProcessedPatients
    );
    console.log(query);
    const response = await connection.query(query);
    console.log(response.rowCount);
    let next = link.find((l) => l.relation === "next");
    if (next && next.url) {
      do {
        const url = next.url;
        const {
          data: { entry: patients, link },
        } = await axios.get(url, {
          auth: {
            username: process.env.SMILE_CDR_USERNAME,
            password: process.env.SMILE_CDR_PASSWORD,
          },
        });
        const currentPatients = await processPatients(patients);
        const currentProcessedPatients = uniqBy(currentPatients, (x) => x[0]);
        const q = format(
          `INSERT INTO staging_patient (case_id,sex,date_of_birth,deceased,date_of_death,facility_id) VALUES %L ON CONFLICT (case_id) DO UPDATE 
            SET sex = EXCLUDED.sex,date_of_birth = EXCLUDED.date_of_birth,deceased = EXCLUDED.deceased,date_of_death = EXCLUDED.date_of_death,facility_id = EXCLUDED.facility_id;`,
          currentProcessedPatients
        );
        const response = await connection.query(q);
        console.log(response.rowCount);
        next = link.find((l) => l.relation === "next");
      } while (!!next);
    }

    connection.release();
    return response;
  } catch (error) {
    console.log(error.message);
    connection.release();
  }
}

async function queryObservations(params) {
  const connection = await pool.connect();
  const {
    data: { entry: observations, link },
  } = await axios.get("http://smilecdr.mets.or.ug:8000/fhir/Observation", {
    params,
    auth: {
      username: process.env.SMILE_CDR_USERNAME,
      password: process.env.SMILE_CDR_PASSWORD,
    },
  });
  let initialObs = await insertObs(observations);
  initialObs = uniqBy(initialObs, (x) => `${x[1]}${x[2]}`);
  const r1 = await connection.query(
    format(
      "INSERT INTO staging_patient_obs(case_id,encounter_id,concept_uuid,concept_name,concept_value,effective_date) VALUES %L",
      initialObs
    )
  );
  console.log(r1.rowCount);
  let next = link.find((l) => l.relation === "next");
  if (next && next.url) {
    do {
      const url = next.url;
      const {
        data: { entry: observations, link },
      } = await axios.get(url, {
        auth: {
          username: process.env.SMILE_CDR_USERNAME,
          password: process.env.SMILE_CDR_PASSWORD,
        },
      });
      let currentObs = await insertObs(observations);
      const r1 = await connection.query(
        format(
          "INSERT INTO staging_patient_obs(case_id,encounter_id,concept_uuid,concept_name,concept_value,effective_date) VALUES %L",
          currentObs
        )
      );
      console.log(r1.rowCount);
      next = link.find((l) => l.relation === "next");
    } while (!!next);
  }
  connection.release();
}

if (args.length > 0) {
  const command = args[0];
  const _count = args.length === 2 ? Number(args[1]) : 250;

  if (command === "patients") {
    queryPatients({ _count }).then(() => console.log("Done Patients"));
  } else if (command === "obs") {
    queryObservations({ _count }).then(() => console.log("Done Observations"));
  } else {
    console.log(`Invalid option ${command} expected obs or patients`);
  }
}
