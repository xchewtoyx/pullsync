from contextlib import contextmanager
import json
import os
import re
import time

from cement.core import controller, handler
from dateutil.parser import parse as parse_date

class TodoSync(controller.CementBaseController):
    class Meta:
        label = 'todosync'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--todo', '-f'], {
                'help': 'Location of todo.txt file',
                'required': True,
            }),
        ]

    @contextmanager
    def _todo_lock(self):
        self.app.redis.setnx('lock:todo', True)
        self.app.redis.expire('lock:todo', 30)
        yield None
        self.app.redis.delete('lock:todo')

    def read_todo_file(self):
        todofile = self.app.pargs.todo
        id_pattern = re.compile(r'\[(\d+)\]')
        with open(todofile, 'r') as todo_list:
            for line in todo_list:
                match = id_pattern.search(line)
                if match:
                    entry_id = int(match.group(1))
                else:
                    entry_id = None
                yield entry_id, line.strip()

    def fetch_todo_entries(self):
        seen = set()
        entry = {}
        extra = []
        for entry_id, line in self.read_todo_file():
            if entry_id:
                seen.add(entry_id)
                entry[entry_id] = line
            else:
                extra.append(line)
        return seen, entry, extra

    def weighted_pulls(self):
        new_items = self.app.pulldb.list_unread()
        for pull in new_items:
            pull_detail = json.loads(self.app.redis.get(pull))
            if not pull_detail.get('stream_id'):
                weight = 2.0
            else:
                weight = float(pull_detail['weight'])
            yield weight, pull, pull_detail

    def write_todo_file(self, entries):
        backup_name = '%s.%s' % (
            self.app.pargs.todo,
            time.time(),
        )
        os.rename(self.app.pargs.todo, backup_name)
        with open(self.app.pargs.todo, 'w') as todo_file:
            todo_file.write('\n'.join(entries))

    @controller.expose(hide=True)
    def default(self):
        with self._todo_lock():
            seen, entry, extra  = self.fetch_todo_entries()
            handled = set()
            new_entries = []
            pulls = sorted(self.weighted_pulls())
            for weight, pull, pull_detail in pulls:
                pull_id = int(pull_detail['identifier'])
                if pull_id in handled:
                    self.app.log.info(
                        'Skipping already handled pull %d' % pull_id
                    )
                    continue
                if pull_id in seen:
                    new_entries.append(entry[pull_id])
                else:
                    new_entries.append('%s +%s {%s} [%s]' % (
                        pull_detail['name'],
                        pull_detail.get('stream_id'),
                        pull_detail['volume_id'],
                        pull_detail['identifier'],
                    ))
                handled.add(pull_id)
            unknown = seen - handled
            for pull_id in unknown:
                new_entries.append(entry[pull_id])
            new_entries.extend(extra)
            self.write_todo_file(new_entries)

def load():
    handler.register(TodoSync)