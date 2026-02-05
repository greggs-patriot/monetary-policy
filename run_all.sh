set -e  # exit on first error

python3 scripts/general/process.py
python3 scripts/long_term_repos/process_long_repos.py
python3 scripts/long_term_repos/process_long_repos_2.py
python3 scripts/long_term_repos/process_long_repos_3.py
python3 scripts/long_term_repos/agg.py