"""Run the indexed UPDATE regression against a built MiniSQL CLI."""
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


class IndexedUpdateTest(unittest.TestCase):
    def test_rebuilt_name_index(self):
        commands = [
            "create database db0;",
            "use db0;",
            "create table account(id int, name char(16) unique, balance float, primary key(id));",
            'insert into account values (2500, "acct002500", 25.00);',
            'insert into account values (2501, "acct002501", 25.01);',
            "drop index account_unique_index_name;",
            "update account set balance = 0 where id = 2500;",
            "create index idx01 on account(name);",
            'update account set id = 2501 where name = "acct002500";',
            'update account set name = "acct002501" where id = 2500;',
            "select * from account where id = 2500;",
            'update account set id = 100001, balance = 88.50 where name = "acct002500";',
            "select * from account where id = 100001;",
            'select * from account where name = "acct002500";',
            "select * from account where id = 2500;",
            "delete from account where id = 100001;",
            'select * from account where name = "acct002500";',
            "quit;",
        ]
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([binary], cwd=directory, input="\n".join(commands) + "\n",
                                    capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        outputs = result.stdout.split("minisql > ")[1:]
        self.assertEqual(len(outputs), len(commands))
        self.assertIn("Missing constraint index for column name", outputs[6])
        self.assertIn("Duplicate entry for key account_primary_index", outputs[8])
        self.assertIn("Duplicate entry for key idx01", outputs[9])
        self.assertRegex(outputs[10], r"\|\s*2500\s*\|\s*acct002500\s*\|\s*25\.000000\s*\|")
        self.assertIn("1 row affected", outputs[11])
        updated = re.compile(r"\|\s*100001\s*\|\s*acct002500\s*\|\s*88\.500000\s*\|")
        for output in outputs[12:14]:
            self.assertRegex(output, updated)
            self.assertIn("1 row in set", output)
        self.assertIn("Empty set", outputs[14])
        self.assertIn("1 row affected", outputs[15])
        self.assertIn("Empty set", outputs[16])


if __name__ == "__main__":
    binary = Path(sys.argv.pop(1)).resolve()
    unittest.main()
