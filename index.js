const fs = require("fs");
const { config } = require("dotenv");
const axios = require("axios");
const JSONStream = require("jsonstream");
const split = require("split");
config();

const elasticUrl = process.env.ELASTIC_URL;
const username = process.env.ELASTIC_USERNAME;
const password = process.env.ELASTIC_PASSWORD;

const path = require("path");

async function insertIntoElastic(obj, indexName) {
  try {
    console.log(`${elasticUrl}/${indexName}/_doc`);
    const result = await axios.post(`${elasticUrl}/${indexName}/_doc`, obj, {
      auth: {
        username: username,
        password: password,
      },
    });
    if (result.status != 201 && result.status != 200) {
      throw new Error("failed to insert into Elastic");
    } else console.log(result.status);
  } catch (error) {
    console.log(
      "=================== in insert into elastic ===================="
    );
    console.log(error);
    console.log(error.response);
    // process.exit(1);
  }
}

function insertEthiopianSoilMapData() {
  const ethiopian_soil = JSON.parse(
    fs.readFileSync("Vector - Ethiopia_Soil_Data_wgs84 (1).geojson").toString()
  );
  ethiopian_soil.features.forEach(async (rec, indx) => {
    let payload = {
      location: rec.geometry,
    };
    setTimeout(
      async () => await insertIntoElastic(payload, "ethiopian_soil"),
      indx * 1000
    );
  });
}

function insertBonkeBoundaryMapData() {
  const bonke_boundary = JSON.parse(
    fs.readFileSync("raster Bonke_boundary_wgs84 (2).geojson").toString()
  );
  bonke_boundary.features.forEach(async (rec) => {
    let payload = {
      region_name: rec.properties.R_NAME,
      zone_name: rec.properties.Z_NAME,
      woreda_name: rec.properties.W_NAME,
      location: rec.geometry,
    };
    await insertIntoElastic(payload);
  });
}

function insertKCLDemoMap() {
  const KCLDemoMap = JSON.parse(
    fs
      .readFileSync(
        "Tabular - KCL demo data - Amhara - edited - KCL demo data - Amhara - edited cleaned (1).geojson"
      )
      .toString()
  );

  KCLDemoMap.features.forEach(async (rec, indx) => {
    let payload = {
      region_name: rec.properties.Region,
      kebele_name: rec.properties.Kebele,
      woreda_name: rec.properties.Wereda,
      location: rec.geometry,
    };
    setTimeout(async () => {
      await insertIntoElastic(payload, "kcl_demo");
    }, indx * 1000);
  });
}

async function basinData() {
  let basin = JSON.parse(fs.readFileSync("./basin.geojson").toString());
  basin.features.forEach(async (feat) => {
    let payload = { location: feat.geometry };
    console.log(payload);
    await insertIntoElastic(payload, "basin");
  });
}

async function soilMapData() {
  let mapData = JSON.parse(fs.readFileSync("./soil.geojson").toString());
  mapData.features.forEach(async (feat, indx) => {
    let payload = {
      location: feat.geometry,
      name: feat.properties.WS_Name,
      area: feat.properties.area_ha,
      scheme_name: feat.properties.Scheme_nam,
      region_name: feat.properties.Region,
      record_type: "PASDIP",
    };

    setTimeout(async () => {
      await insertIntoElastic(payload, "soil_pasdip_map_data");
    }, 300 * indx);
  });
}

async function bioPasdipMapData() {
  let mapData = JSON.parse(fs.readFileSync("./bio.geojson").toString());
  mapData.features.forEach(async (feat, indx) => {
    let payload = {
      location: feat.geometry,

      area: feat.properties["AREA"],
      region_name: feat.properties["REGION"],
      treatment_type: "Biological",
      treatment: feat.properties["Field1"],
      woreda_name: feat.properties["WEREDA"],
      zone_name: feat.properties["ZONES"],
      record_type: "PASDIP",
    };

    setTimeout(async () => {
      await insertIntoElastic(payload, "biological_pasdip_map_data");
    }, 300 * indx);
  });
}

async function phyPasdipMapData() {
  let mapData = JSON.parse(fs.readFileSync("./phy.geojson").toString());
  mapData.features.forEach(async (feat, indx) => {
    let payload = {
      location: feat.geometry,
      name: feat.properties["BASIN_NAME"],
      area: feat.properties["AREA"],
      treatment_type: "Physical",
      record_type: "PASDIP",
    };
    setTimeout(async () => {
      await insertIntoElastic(payload, "physical_pasdip_map_data");
    }, 300 * indx);
  });
}

async function slmpMapData() {
  let mapData = JSON.parse(
    fs.readFileSync("./slmp_map_data.geojson").toString()
  );

  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      region_name: mapData["features"][x]["properties"]["R_NAME"],
      zone_name: mapData["features"][x]["properties"]["Z_NAME"],
      woreda_name: mapData["features"][x]["properties"]["W_NAME"],
      area: mapData["features"][x]["properties"]["Area"],
      name: /\//.test(mapData["features"][x]["properties"]["Basin"])
        ? mapData["features"][x]["properties"]["Basin"].split("/")[0]
        : mapData["features"][x]["properties"]["Basin"],
      microwatershed: mapData["features"][x]["properties"]["mws_nm"],
      majorwatershed: mapData["features"][x]["properties"]["cws_nm"],
      location: mapData["features"][x]["geometry"],
      year: mapData["features"][x]["properties"]["imp_yr"],
      string_year: String(mapData["features"][x]["properties"]["imp_yr"]),
      date: mapData["features"][x]["properties"]["imp_year"],
    };
    console.log(payload);

    setTimeout(async () => {
      await insertIntoElastic(payload, "slmp_map_data");
    }, 300 * x);
  }
}

// slmpMapData()
//   .then()
//   .catch((err) => console.log(err));

/**
 * 
 *     let treatments = Object.keys(mapData["features"][x]["properties"]).slice(
       29
     );

     treatments.forEach(async (treatment,indx) => {

       let payload = {
         ...mapData["features"][x]["properties"],
         "Major Watershed": mapData["features"][x]["properties"]["cws_nm"],
         "Micro Watershed": mapData["features"][x]["properties"]["mws_nm"],
         region_name: mapData["features"][x]["properties"]["R_NAME"],
         zone_name: mapData["features"][x]["properties"]["Z_NAME"],
         woreda_name: mapData["features"][x]["properties"]["W_NAME"],
         area: mapData["features"][x]["properties"]["Area"],
         year: mapData["features"][x]["properties"]["imp_yr"],
         string_year: String(mapData["features"][x]["properties"]["imp_yr"]),
         date: mapData["features"][x]["properties"]["imp_year"],
         location: mapData["features"][x]["geometry"],
        treatment,
        area: mapData["features"][x]["properties"][treatment]
       };
     })

 */
async function slmp_physical_map_data() {
  const normalizeTreatmentName = (treatment) => {
    // Hillsideterracing: mapData["features"][x]["properties"]["Hillsidete"],
    // Benchterracing: mapData["features"][x]["properties"]["Benchterra"],
    // Percolation: mapData["features"][x]["properties"]["Percolatio"],
    // Cuttoffdrain: mapData["features"][x]["properties"]["Cutoffdrai"],
    // Eyebrowbasin: mapData["features"][x]["properties"]["Eyebrowbas"],
    // HillsideDitching: mapData["features"][x]["properties"]["Hillsidede"],
    // GabionCheckDam: mapData["features"][x]["properties"]["Gabionchec"],
    // BrushwoodCheckDam: mapData["features"][x]["properties"]["Brushwoodc"],
    // SandbagCheckDam: mapData["features"][x]["properties"]["Sandbagche"],
    // Gullyrevegetation: mapData["features"][x]["properties"]["Gullyreveg"],
    // PlasticGabion: mapData["features"][x]["properties"]["Gullyreveg"],
    // FanyaJu: mapData["features"][x]["properties"]["Fanyajuuco"],
    let normalizedValue = "";
    switch (treatment) {
      case "Hillsidete":
        normalizedValue = "Hillsideterracing";
        break;
      case "Benchterra":
        normalizedValue = "Benchterracing";
        break;
      case "Percolatio":
        normalizedValue = "Percolation";
        break;
      case "Cutoffdrai":
        normalizedValue = "Cuttoffdrain";
        break;
      case "Eyebrowbas":
        normalizedValue = "Eyebrowbasin";
        break;
      case "Hillsidede":
        normalizedValue = "HillsideDitching";
        break;
      case "Gabionchec":
        normalizedValue = "GabionCheckDam";
        break;
      case "Brushwoodc":
        normalizedValue = "BrushwoodCheckDam";
        break;
      case "Sandbagche":
        normalizedValue = "SandbagCheckDam";
        break;
      case "Gullyreveg":
        normalizedValue = "Gullyrevegetation";
        break;
      case "Fanyajuuco":
        normalizedValue = "FanyaJu";
        break;
      case "Plasticgab":
        normalizedValue = "PlasticGabion";
        break;
    }
    return normalizedValue;
  };
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./new_physical_kpi.geojson")).toString()
  );

  for (let x = 0; x < mapData["features"].length; x++) {
    let treatments = Object.keys(mapData["features"][x]["properties"]).slice(
      29
    );

    treatments.forEach(async (treatment, indx) => {
      let payload = {
        ...mapData["features"][x]["properties"],
        "Major Watershed": mapData["features"][x]["properties"]["cws_nm"],
        "Micro Watershed": mapData["features"][x]["properties"]["mws_nm"],
        region_name: mapData["features"][x]["properties"]["R_NAME"],
        zone_name: mapData["features"][x]["properties"]["Z_NAME"],
        woreda_name: mapData["features"][x]["properties"]["W_NAME"],
        area: mapData["features"][x]["properties"]["Area"],
        year: mapData["features"][x]["properties"]["imp_yr"],
        string_year: String(mapData["features"][x]["properties"]["imp_yr"]),
        date: mapData["features"][x]["properties"]["imp_year"],
        location: mapData["features"][x]["geometry"],

        treatment: normalizeTreatmentName(treatment),
        treatment_type: "Physical",
        record_type: "SLMP",
        name: mapData["features"][x]["properties"]["Basin"].split("/")[0],
      };
      setTimeout(async () => {
        await insertIntoElastic(
          payload,
          "slmp_physcial_swc_treaments_map_data",
          `${payload.date}-${payload.treatment}`
        );
      }, 300 * x + indx);
    });
  }
}

async function slmp_biological_data() {
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./new_biological_slmp.geojson")).toString()
  );

  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      ...mapData["features"][x],
    };
    console.log(payload);
  }
}

//physical_meret_map_data
async function PhysicalMeretData() {
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./phy_meret.geojson")).toString()
  );
  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      ...mapData["features"][x]["properties"],
      treatment: mapData["features"][x]["properties"]["Field1"],
      region_name: mapData["features"][x]["properties"]["REGION"],
      woerda_name: mapData["features"][x]["properties"]["WEREDA"],
      zone_name: mapData["features"][x]["properties"]["ZONES"],
      location: mapData["features"][x]["geometry"],
    };

    setTimeout(async function () {
      await insertIntoElastic(payload, "physical_meret_map_data");
    }, 300 * x);
  }
}

//biological_meret_map_data
async function BiologicalMeretData() {
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./bio_merert.geojson")).toString()
  );
  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      ...mapData["features"][x]["properties"],
      region_name: mapData["features"][x]["properties"]["REGION"],
      woreda_name: mapData["features"][x]["properties"]["WEREDA"],
      zone_name: mapData["features"][x]["properties"]["ZONES"],
      treament: mapData["features"][x]["properties"]["Field1"],
      location: mapData["features"][x]["geometry"],
    };
    setTimeout(async function () {
      await insertIntoElastic(payload, "biological_physical_map_data");
    }, 300 * x);
  }
}

async function EhtioBasin() {
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./ethio_basin.geojson")).toString()
  );
  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      ...mapData["features"][x]["properties"],
      location: mapData["features"][x]["geometry"],
    };
    setTimeout(async function () {
      await insertIntoElastic(payload, "ethio_basin");
    }, 300 * x);
  }
}
async function soilPHmapdata() {
  let mapData = JSON.parse(
    fs.readFileSync(path.join("./tif", "new_ph_rf.geojson")).toString()
  );
  for (let x = 0; x < mapData["features"].length; x++) {
    let payload = {
      location: mapData["features"][x]["geometry"],
    };

    setTimeout(async () => {
      await insertIntoElastic(payload, "soil_ph_map_data");
    }, 300 * x);
    // console.log(response.status)
  }
}

async function newSoilMapData() {
  let stream = fs.createReadStream(path.join("./tif", "new_S_rf.geojson"));
  const splitStream = stream.pipe(split());
  const jsonStream = splitStream.pipe(JSONStream.parse("*"));

  jsonStream.on("data", (data) => {
    // Process your JSON object here
    console.log(data);
  });

  // Handle errors
  jsonStream.on("error", (error) => {
    console.error("Error parsing JSON:", error.message);
  });

  // Handle the end of the stream
  jsonStream.on("end", () => {
    console.log("Stream ended");
  });
  // for (let x = 0; x < mapData["features"].length; x++) {
  //   // let payload = {
  //   //   location: mapData["features"][x]["geometry"],
  //   // };
  //   console.log(mapData["features"][x]);
  //   break;
  // }
}
(async () => {
  await soilPHmapdata();
  // await newSoilMapData();
})();

//pgD5WP
// EhtioBasin()
//   .then()
//   .catch((err) => console.log(err));
// BiologicalMeretData()
//   .then()
//   .catch((err) => console.log(err));
// PhysicalMeretData()
//   .then()
//   .catch((err) => console.log(err));
// slmp_biological_data().then().catch();
// slmp_physical_map_data().then().catch();

// phyPasdipMapData()
//   .then()
//   .catch((err) => console.log(err));
// bioPasdipMapData()
//   .then()
//   .catch((err) => console.log(err));
// soilMapData()
//   .then()
//   .catch((err) => console.log(err));
// insertKCLDemoMap();
// insertEthiopianSoilMapData();
// basinData()
//   .then()
//   .catch((err) => console.log(err));
