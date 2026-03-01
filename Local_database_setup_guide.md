# Labor Certification Database Setup

## Install PostgreSQL

```bash
brew install postgresql@16
brew services start postgresql@16
```

## Create Python Virtual Environment (Optional)

```bash
# Create venv
python -m venv venv

# Activate venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# When done, deactivate
deactivate
```

## Setup Database

```bash
./setup_database.sh
```

## Connect

```bash
psql -d labor_certification
```

## Example Queries

```sql
-- View employers
SELECT * FROM employer LIMIT 10;
```
