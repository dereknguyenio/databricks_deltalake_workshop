# 🎓 Azure Databricks Delta Lake Hands-On Workshop

Welcome to the **Azure Databricks Delta Lake workshop**! This hands-on lab will guide you through ingestion, Delta Lake best practices, data optimization, schema evolution, liquid clustering, concurrency management, and security features—all using **Unity Catalog** and **ADLS Gen2**.

**Target Audience:** Data engineers, architects, and platform administrators looking to deepen their Databricks and Delta Lake skills.

---

## 📋 Prerequisites

Before participating, ensure the following requirements are completed by your administrator:

### 🔐 Unity Catalog Permissions

Participants must have the following permissions:

| Permission | Scope | Purpose |
|------------|-------|---------|
| `USAGE` | Catalog | Allows access to the catalog |
| `USAGE` | Schema | Allows access to schema |
| `CREATE` | Schema | Allows users to create their own schema |
| `CREATE TABLE`, `READ FILES`, `WRITE FILES` | External Location | Allows reading/writing to ADLS-backed Delta paths |

**Example grants (admin only):**

```sql
GRANT USAGE ON CATALOG <catalog> TO <group>;
GRANT USAGE, CREATE ON DATABASE <catalog>.db1 TO <group>;
GRANT CREATE TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION <location> TO <group>;
```

### 🖥️ Workspace Permissions

- ✅ Users automatically receive a personal workspace folder in `/Workspace/Users/<email>`  
  → *No admin action needed*
- ✅ Users need **Can Attach To** on the workshop cluster
- ✅ Users do **not** need cluster edit permissions

### 🗂️ Storage Permissions (ADLS Gen2)

If users will upload their own files:

**Recommended role at the container level:**  
`Storage Blob Data Contributor`

This allows:
- ✔ Browsing folders
- ✔ Uploading example files
- ✔ Reading Delta logs
- ✔ Interacting with Auto Loader paths

---

## 🧱 Workshop Structure

This repository contains all notebooks/scripts in the order they will be delivered. Below is the module-by-module breakdown.

---

## 🚀 Module 01 – ADLS Gen2 Setup

**Service Principal Mount / External Location**

**Files:**
- `01-adls-gen2-service-principal-mount.py`

**Topics:**
- How ADLS Gen2 is secured
- External locations vs mounts
- Storage credentials (Service principal, SAS, MSI)
- Unity Catalog access enforcement

---

## 📥 Modules 02–06: Data Ingestion

### 02 – Ingest CSV via COPY INTO

**Files:**
- `02-Ingest-CSV.sql`

**Topics:**
- CSV ingestion with schema inference
- Handling corrupt records
- COPY INTO idempotency

### 03 – Ingest SQL

**Files:**
- `03-Ingest-SQL.sql`

**Topics:**
- Reading relational data into Delta
- Overwrite vs append

### 04 – Ingest COPY INTO (Advanced)

**Files:**
- `04-Ingest-CopyInto.sql`

### 05 – Ingest via Auto Loader (Python + Scala)

**Files:**
- `05-Ingest-Autoloader.scala`
- `06_Batch_Reads_Writes.scala`

**Topics:**
- Automatic discovery of new files
- Checkpointing
- Schema evolution

---

## 🔄 Module 07 – Change Data Feed (CDF)

**Files:**
- `07-Change data feed demo.py`
- `Delta Change Data Feed for Seamless CDC Queries.sql`

**Topics:**
- Enabling CDF
- Querying changes over time
- Insert/update/delete tracking
- Downstream SCD patterns

---

## ⚙️ Modules 08–10: Table Management

### 08 – OPTIMIZE (Bin-packing & Z-Order)

**Files:**
- `8_Optimize.sql`

### 09 – VACUUM (Retention, Risks, Recovery)

**Files:**
- `9_Vacuum.sql`

### 10 – Table Metadata / Describe Detail

**Files:**
- Embedded in other notebooks

**Topics:**
- How Z-Ordering works
- Liquid clustering comparison
- How Vacuum interacts with deletion vectors
- Retention tuning for CDF and logs

---

## 🧬 Module 11 – Cloning (Shallow vs Deep)

**Files:**
- `11_Clone.sql`

**Concepts:**
- Instant cloning
- Zero-copy clones
- Storage savings
- Lifecycle isolation

---

## 📦 Module 12 – Parquet to Delta Conversion

**Files:**
- `12-Delta-Parquet.py`

**Topics:**
- Converting existing file-based datasets to Delta
- Benefits vs Parquet
- Time travel

---

## 💥 Module 13 – Restore & Time Travel

**Files:**
- `13-Restore-Recover-Table.sql`

**Topics:**
- Delta transaction history
- Rolling back accidental deletes
- Data recovery patterns

---

## 🔌 Module 14 – Concurrency & ACID Guarantees

**Files:**
- `14_Concurrency_Test.py`
- `14_Concurrency_Test2.py`

**Concepts:**
- Write serializable isolation
- Common concurrency conflicts
- How Delta handles readers vs writers
- Demonstration of conflict exceptions

---

## 📊 Module 15 – Data Skipping & File Skipping

**Files:**
- `15-DataSkipping.sql`

**Topics:**
- Data skipping statistics
- File pruning
- Index metadata

---

## 📏 Module 16 – File Size Management

**Files:**
- `16_FileSizeManagement.sql`

**Topics:**
- Small file problem
- Auto-compaction
- WRITE_OPTIONS
- Optimize Write

---

## ⚡ Modules 17–18: Liquid Clustering

**Files:**
- `17-Optimize-Zorder.sql`
- `18_Partitioning_Liquid_Cluster.sql`

**Topics:**
- How Liquid Clustering works
- Partitionless layout
- Rewriting behavior
- When to use LC over Z-Order

---

## 🌸 Module 19 – Bloom Filter Index

**Files:**
- `19_Bloomfilter.sql`

**Topics:**
- Improving point lookup performance
- FPP tuning
- Index file creation

---

## 🔐 Module 21 – Row-Level Security (RLS)

**Files:**
- `21-Row-Level-Security.py`

**Topics:**
- Dynamic filters
- Policy functions
- Multi-tenant enforcement
- Unity Catalog governance patterns

---

## 🧹 Module 22 – Deletion Vectors

**Files:**
- `22_Deletion_Vectors.py`

**Topics:**
- How DV stores delete metadata
- Differences vs file rewrites
- Performance benefits
- DV + Liquid Clustering

---

## 🔒 Permissions & Setup Reference

**Files:**
- `Permissions.py`

**Contains:**
- Quick commands for admins
- UC grants
- Storage permissions
- Cluster attach permissions

---

## 📘 How To Use These Notebooks

### 1. Open Databricks Workspace

Navigate to your personal workspace folder:
```
/Workspace/Users/<your email>/
```

### 2. Import all workshop files

You may drag-and-drop or import from Git.

### 3. Start the workshop cluster

Your admin will provide the cluster name. Ensure you have **Can Attach To** permission.

### 4. Run notebooks in numeric order

**Example:**  
`01` → `02` → `03` → `...` → `22`

### 5. Every notebook automatically creates its own ADLS path

Each notebook uses:

```python
username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
adls_path = f"{base_path}/{username}"
```

This ensures:
- ✔ No collisions
- ✔ Private workspace for each user
- ✔ Consistent path across all notebooks

---

## 🎯 Expected Outcomes

By the end of this workshop, participants will be able to:

- ✔ Ingest batch & streaming data into Delta
- ✔ Use Delta Lake features (Optimize, Vacuum, Time Travel)
- ✔ Understand partitions, Z-Order, and Liquid Clustering
- ✔ Handle schema evolution & enforcement
- ✔ Set up Change Data Feed
- ✔ Understand concurrency & ACID guarantees
- ✔ Implement Data Skipping & Bloom Indexes
- ✔ Apply Row-Level Security in Unity Catalog
- ✔ Manage ADLS-backed Lakehouse structures

---

## 📞 Support

If you need help during the workshop, please contact your **Microsoft Databricks session leads** or raise your hand during the lab.

---

**Happy Learning! 🚀**
