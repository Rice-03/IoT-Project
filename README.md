# IoT Environmental Monitoring: Data Team Pipeline

This repo holds the **data processing pipeline** for our IoT project. Sensor nodes (ESP32s) on campus measure the environment and send readings over MQTT. Our job is to **receive those readings, clean them, calculate useful values from them, and hand them to the backend team**, who store them in TimescaleDB and serve them to the frontend.

We're building this like a puzzle: each person builds one piece, tests it on its own, and uploads it. The next person downloads everything before them and builds on top.

---

## Where we fit in the whole project

```
Hardware team      Network team        DATA TEAM (this repo)                 Backend team            Frontend team
ESP32 + sensors -> MQTT broker ------> receive -> clean -> calculate -----> TimescaleDB + API ----> dashboard
```

We do **not** touch the database. Backend owns that. We finish when a clean, calculated reading is handed over.

---

## The pipeline, stage by stage

| Stage | What it does | File | Owner | Status |
|---|---|---|---|---|
| 1 | Connect to MQTT, receive readings, drop broken JSON | `mqtt_consumer.py` | Raeez Ahmed | Done |
| 2a | Cleaning: is the reading's **structure** OK? | `clean_structure.py` | [name] | To do |
| 2b | Cleaning: are the **values** realistic? | `clean_values.py` | [name] | To do |
| 3 | Calculations: heat index, dew point, absolute humidity, AQI | `calculations.py` | [name] | To do |
| 4 | Wire all stages together, hand off to backend | `pipeline.py` | [name] | To do (last) |

(Stage 1 and 4 are sections 3.1 and 3.2 in the Task Breakdown PDF.)

**Order of work:**

1. Stage 1 is uploaded (this is where we are now).
2. Cleaning pair downloads the repo, builds 2a and 2b, uploads them.
3. Calculations downloads the repo (now with 1, 2a, 2b), builds stage 3, uploads it.
4. Stage 4 connects everything into one running pipeline.

You don't have to wait for the stage before yours to be *finished* to start. The `sample_data/` folder has example data so you can build against it right away (see "How the stages connect" below).

---

## Repo structure

```
.
├── README.md                  <- you are here
├── requirements.txt           <- Python packages to install
├── .gitignore                 <- stops junk files being uploaded
│
├── fake_broker.py             <- fake MQTT broker + fake sensor (testing tool)
├── mqtt_consumer.py           <- Stage 1
├── test_stage1.py             <- test for Stage 1
│
├── clean_structure.py         <- Stage 2a   (to be added)
├── clean_values.py            <- Stage 2b   (to be added)
├── test_cleaning.py           <- tests for 2a + 2b (to be added)
│
├── calculations.py            <- Stage 3    (to be added)
├── test_calculations.py       <- test for Stage 3 (to be added)
│
├── pipeline.py                <- Stage 4    (to be added)
│
└── sample_data/
    └── raw_readings_sample.jsonl   <- 67 example readings from Stage 1
```

**Keep everything in one flat folder** (no subfolders for code). This way every file can use every other file without any import setup. Only data samples go in `sample_data/`.

Please use **exactly these file names**, so stage 4 can find everything.

---

## Getting started (everyone do this first)

**You need:** Python 3.10 or newer, and Git.

1. Clone the repo and open a terminal inside the folder.
2. Install the packages:
   ```
   pip install -r requirements.txt
   ```
3. Check that Stage 1 works on your machine:
   ```
   python test_stage1.py
   ```
   You should see a list of checks ending in **`9/9 checks passed`**. If you see that, your setup is correct. This test starts its own fake broker, so you don't need anything else running.

**Windows tip:** if `python` does nothing or opens the Microsoft Store, use `py` instead (`py test_stage1.py`, `py -m pip install -r requirements.txt`).

**OneDrive tip:** avoid keeping this repo inside a OneDrive folder. OneDrive can leave files at 0 bytes while syncing, and the code then silently does nothing. Run `dir` (Windows) or `ls -l` (Mac/Linux) and check the files have real sizes. A plain folder like `C:\Dev\` is safer.

---

## About `requirements.txt`

It lists the Python packages this project needs. `pip install -r requirements.txt` installs all of them in one go.

Right now it has two:

- **amqtt**: lets `fake_broker.py` run an MQTT broker in pure Python, so nobody needs to install Mosquitto or Docker.
- **paho-mqtt**: the standard Python library for connecting to an MQTT broker. Stage 1 uses it.

Cleaning and calculations should only need Python's built-in modules (like `json`, `math`, `datetime`), so you probably won't need to add anything. **If you do add a package**, add it to `requirements.txt` in the same commit and tell the group, otherwise the next person's code will crash with `ModuleNotFoundError`.

---

## Running the fake broker and Stage 1 yourself

The real sensors and the network team's broker aren't ready yet, so `fake_broker.py` stands in for both. It runs a broker on your own computer and pretends to be a sensor node, sending a reading every 5 seconds.

**You need two terminals, both open in the repo folder.**

**Terminal 1** (start this first):
```
python fake_broker.py
```
It waits 3 seconds before sending the first reading, so you have time to start terminal 2.

**Terminal 2:**
```
python mqtt_consumer.py
```
You'll see readings arrive, one line each. Every reading is also saved to a file called `raw_readings.jsonl` in the same folder.

**To stop:** click into each terminal and press **Ctrl+C**. Each prints a short "stopped" message.

**Optional settings for the fake broker:**

| Command | What it does |
|---|---|
| `python fake_broker.py --interval 1` | Send a reading every 1 second instead of 5 |
| `python fake_broker.py --clean-only` | Never send broken readings |
| `python fake_broker.py --delay 0` | Start sending immediately |

### The fake broker sends broken readings on purpose

About every 4th reading is deliberately broken, so cleaning has real problems to deal with. Terminal 1 labels each one (e.g. `BAD (missing_id)`). Here's every type, grouped by which stage should catch it:

| Broken case | What's wrong | Who handles it |
|---|---|---|
| `malformed_json` | Not valid JSON at all | Stage 1 already drops it. You'll never see it. |
| `missing_id` | No `id` field | 2a (structure) |
| `missing_timestamp` | No `ts` field | 2a (structure) |
| `humidity_as_string` | `humidity` is text ("sixty"), not a number | 2a (structure) |
| `temp_out_of_range` | `temperature` is 250 | 2b (values) |
| `negative_pm25` | `pm25` is below 0 | 2b (values) |
| `pm25_null` | `pm25` is empty (null) | 2b should leave it as null. Calculations must cope with it. |

---

## What a reading looks like

Every reading coming out of Stage 1 is one **dictionary** (a JSON object) with these fields:

| Field | Meaning | Unit |
|---|---|---|
| `id` | Which sensor node sent it | text, e.g. `device-01` |
| `ts` | When the sensor took the reading | ISO timestamp, UTC |
| `temperature` | Air temperature | °C |
| `humidity` | Relative humidity | % (0 to 100) |
| `pressure` | Air pressure | hPa |
| `pm25` | Fine dust particles | µg/m³ |
| `tvoc` | Volatile organic compounds | ppb |
| `eco2` | Estimated CO2 (from the ENS160) | ppm |
| `co2` | Measured CO2 (from the CO2 sensor) | ppm |
| `battery_v` | Node battery voltage | volts |
| `lat`, `lon` | GPS position | degrees |
| `altitude` | GPS altitude | metres |
| `received_at` | When **Stage 1** received it (added by Stage 1) | ISO timestamp, UTC |
| `source_topic` | MQTT topic it arrived on (added by Stage 1) | text |

Two important notes:

- **A missing sensor is `null`, never `0`.** 0°C and "no reading" are very different things. Please keep it that way in every stage.
- These field names are **our assumption** of what the network team will send. If their final format differs, only Stage 1 changes, and everyone else keeps working with the names above.

---

## How the stages connect

Every stage follows the same simple rule:

> **Each stage has one main function. It takes in one reading (a dictionary) and gives back one reading (a dictionary), or gives back `None` if the reading should be thrown away.**

That's the whole contract. If everyone sticks to it, stage 4 can snap the pieces together like Lego without anyone changing their code.

| Stage | File | Main function | Takes in | Gives back |
|---|---|---|---|---|
| 2a | `clean_structure.py` | `clean_structure` | reading from Stage 1 | the reading, or `None` if rejected |
| 2b | `clean_values.py` | `clean_values` | reading from 2a | the reading with bad values set to `null` |
| 3 | `calculations.py` | `add_calculations` | reading from 2b | the reading with 4 new fields added |

**Rules for every stage:**

- **Never crash on a weird reading.** Assume anything can arrive. If something's unexpected, handle it (reject it, or set a value to null) rather than letting an error escape.
- **Only add or change fields, never rename existing ones.** The next stage expects the names in the table above.
- **Only edit your own files.** If you need something changed in someone else's file, ask them. This avoids Git conflicts.

### Two ways to test your stage against real-looking data

**Option A: offline, using files (easiest, start here).**
Each stage reads the file the previous stage produced and writes its own file for the next person:

```
raw_readings.jsonl  ->  [2a]  ->  structure_ok.jsonl  ->  [2b]  ->  clean_readings.jsonl  ->  [3]  ->  processed_readings.jsonl
```

`.jsonl` just means "one JSON reading per line". Please set up your file so that **running it directly** (e.g. `python clean_structure.py`) reads the previous file, runs every line through your function, writes your output file, and prints a short summary (how many kept, how many rejected, and why).

To get the input file you need, either:
- use the sample in `sample_data/` (copy it and rename it, e.g. `sample_data/raw_readings_sample.jsonl` becomes `raw_readings.jsonl`), or
- run the fake broker and Stage 1 for a minute (see above), which creates a fresh `raw_readings.jsonl`.

**Option B: live, straight from Stage 1.**
Once your stage works offline, check it with live data. **Don't edit `mqtt_consumer.py`.** Instead, make your own small test script (e.g. `try_cleaning_live.py`). Stage 1's `MQTTConsumer` accepts a `handler`: whatever function you give it gets called with every reading as it arrives. Give it your function, start the fake broker, run your script, and watch your stage process readings in real time. The `test_stage1.py` file shows this pattern if you want an example.

### Handing your stage to the next person

When your stage works, also commit a **sample of your output** into `sample_data/`, e.g. `sample_data/clean_readings_sample.jsonl`. That way the next person can start building immediately, without running your code first.

---

## Stage guides

### Stage 2a: `clean_structure.py` (structural checks)

Question to answer: **"Is this reading shaped correctly?"**

- `id` and `ts` must exist. If either is missing, **reject** the reading (return `None`).
- `ts` must be a valid timestamp. If not, reject.
- Every sensor field that exists must be a number or `null`. Text like `"sixty"` is wrong. Decide as a pair whether that rejects the reading or just nulls that field (nulling it is usually better, since the other sensors' values are still good).
- Every rejection should say **why**. Suggestion: write rejected readings, plus the reason, to `rejected_readings.jsonl` so we can see how often and why things fail.

### Stage 2b: `clean_values.py` (value checks)

Question to answer: **"Are these numbers realistic?"**

- Check each sensor value against a sensible range. Suggested starting ranges:

| Field | Allowed range |
|---|---|
| `temperature` | -40 to 60 °C |
| `humidity` | 0 to 100 % |
| `pressure` | 870 to 1085 hPa |
| `pm25` | 0 to 1000 µg/m³ |
| `tvoc` | 0 to 65000 ppb |
| `eco2` | 400 to 65000 ppm |
| `co2` | 300 to 40000 ppm (real sensors can read a little under 400) |
| `battery_v` | 2.5 to 4.5 V |
| `lat` | -90 to 90 |
| `lon` | -180 to 180 |

- A value outside its range: **set that one field to `null`**, keep the rest of the reading. One broken sensor shouldn't throw away good data from the others.
- It's useful to record what you changed, e.g. a `flags` field listing which fields were nulled and why. Agree the exact format with calculations so they know about it.
- A value that's already `null` stays `null`. Not an error.

**Note for the pair:** 2a and 2b can be built at the same time. 2b can test against the raw sample directly. Just make sure 2b doesn't crash on readings that 2a would have rejected (e.g. a reading with no `id`).

### Stage 3: `calculations.py`

Adds four new fields to every reading:

| New field | Needs | Method | Unit |
|---|---|---|---|
| `heat_index` | temperature, humidity | NOAA heat index formula | °C |
| `dew_point` | temperature, humidity | Magnus formula | °C |
| `absolute_humidity` | temperature, humidity | Standard formula | g/m³ |
| `aqi` | pm25 | US EPA PM2.5 breakpoint table | index (0 to 500) |

Things to watch:

- **If an input is `null`, the output is `null`.** E.g. `pm25` is null, so `aqi` is null. Never crash, never guess.
- The NOAA heat index formula is written for **Fahrenheit**. Convert °C to °F, apply it, convert back.
- The heat index formula is only meant for warm conditions (roughly above 27°C). Decide what to return below that (NOAA's own guidance uses a simpler formula there), and write the decision in a comment.
- AQI isn't one formula: it's a **lookup table** with a straight line between breakpoints. Use the EPA's published PM2.5 table and note in a comment which version you used.
- Also useful (optional): an `aqi_category` field with the label ("Good", "Moderate", etc.).
- Test each formula against **known correct values** (e.g. from an online calculator or the official tables), not just "it runs".

UV index was removed for now because the UV sensor isn't working. It may be added back later as a separate calculation.

### Stage 4: `pipeline.py`

Runs Stage 1 live and passes every reading through 2a, 2b and 3 in order, then hands the finished reading to backend. Built last, once all the pieces above exist.

---

## Testing: what "done" means for your stage

Before you upload, your stage should:

1. **Have its own test file** that runs on its own and prints clear PASS/FAIL results (like `test_stage1.py` does). Cover good readings **and** every broken case in the fake broker table above.
2. **Run through the whole sample file without crashing.**
3. **Still pass `python test_stage1.py`**, to confirm you haven't broken anything that already worked.

---

## Git workflow (keep it simple)

- **Before you start working:** `git pull` so you have everyone's latest files.
- **Only commit your own files** (plus `requirements.txt` if you added a package, and your sample in `sample_data/`).
- **Don't commit generated data** like `raw_readings.jsonl` in the main folder. The `.gitignore` already blocks these, so `git add .` is safe.
- **Write clear commit messages**, e.g. `Add stage 2a structural checks + tests`.
- **Tell the group chat** when you've uploaded, so the next person knows they can pull.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `python` does nothing or opens the Microsoft Store | Use `py` instead of `python` |
| Running a file prints nothing at all | Check the file isn't 0 bytes (OneDrive issue, see Getting started) |
| `ModuleNotFoundError: No module named 'paho'` (or `amqtt`) | Run `pip install -r requirements.txt` |
| `Port 1883 is already in use` | Another broker is already running: a `fake_broker.py` in another terminal you forgot, or Mosquitto installed on your machine. Stop it and try again. |
| Consumer says `broker not reachable` | Start `fake_broker.py` first, in another terminal |
| Consumer connects but nothing arrives | The fake broker only starts sending after 3 seconds. Also check both are running on the same computer. |
| Terminal seems stuck | That's normal for the broker and consumer: they run until you press Ctrl+C |

---

## Later on: switching to the real network broker

When the network team's broker is ready, only **Stage 1's broker address** changes (the host and port at the top of `mqtt_consumer.py`). Every other stage stays exactly the same, which is the point of everyone sticking to the contract above.
