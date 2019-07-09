import os, fnmatch, re, threading, sublime, sqlite3, shutil, time
from multiprocessing.pool import ThreadPool
from .settings import get_settings_param, GLOBAL_SET

CREATE_LIBS_INFO_SQL = '''
create table if not exists libs_info (
    id int unsigned not null,
    parent_id int unsigned not null,
    folder varchar(256) not null,
    primary key(id)
);
'''

INSERT_FOLDER_INFO = '''
replace into libs_info(id, parent_id, folder) values
(?, ?, ?);
'''

QUERY_FOLDER = '''
select id, parent_id from libs_info where folder = ?;
'''

CREATE_LIBS_SQL = '''
create table if not exists libs (
    id int unsigned not null,
    mod_name varchar(128) not null,
    fun_name varchar(128) not null,
    param_len tinyint(2) not null,
    row_num int unsigned not null,
    completion varchar(256) not null,
    primary key(id, mod_name, fun_name, param_len)
); 
'''

INSERT_LIBS_SQL = '''
replace into libs(id, mod_name, fun_name, param_len, row_num, completion) values 
(?, ?, ?, ?, ?, ?);
'''

DEL_LIBS_SQL = '''
delete from libs where id = ? and mod_name = ?;
'''

QUERY_COMPLETION = '''
select fun_name, param_len, completion from libs where mod_name = ?;
'''

QUERY_ALL_MOD = '''
select distinct mod_name from libs;
'''

QUERY_POSITION = '''
select folder, fun_name, param_len, row_num from libs join libs_info where libs_info.id = libs.id and mod_name = ? and fun_name = ?;
'''

DEL_FOLDER_LIBS_SQL = '''
delete from libs where id in (select id from libs_info where parent_id = ?);
'''

DEL_FOLDER_SQL = '''
delete from libs_info where parent_id = ? or folder = ?;
'''

CREATE_INCLUDE_SQL = '''
create table if not exists includes ( 
    id int unsigned not null,
    file_name varchar ( 128 ) not null, 
    include varchar ( 128 ), 
    primary key (id, file_name, include)
);
'''

INSERT_INCLUDE_INFO_SQL = '''
replace into includes (id, file_name, include) values (?, ?, ?);
'''

CREATE_DEFINE_SQL = '''
create table if not exists defines (
    id int unsigned not null,
    file_name varchar(128) not null,
    define varchar(128) not null,
    primary key (id, file_name, define)
);
'''

INSERT_DEFINE_SQL = '''
replace into defines (id, file_name, define) values (?, ?, ?);
'''

QUERY_DEFINE_SQL = '''
with recursive
    includetree(name) as (
    select ? union all 
    select includes.include from includetree, includes 
    where includes.file_name = includetree.name)
select t.define from includetree, defines t where t.file_name = includetree.name;
'''

CREATE_RECORD_INFO_SQL = '''
create table if not exists records (
    id int unsigned not null,
    file_name varchar (128) not null,
    record varchar(128) not null,
    field varchar(128) not null,
    default_val varchar(128) not null,
    primary key (id, file_name, record, field)
);
'''

INSERT_RECORD_INFO_SQL = '''
replace into records (id, file_name, record, field, default_val) values (?, ?, ?, ?, ?);
'''

QUERY_RECORD_SQL = '''
with recursive
    includetree(name) as (
    select ? union all 
    select includes.include from includetree, includes 
    where includes.file_name = includetree.name)
select t.record from includetree,
(select records.file_name, records.record from records group by records.record) t 
where t.file_name = includetree.name;
'''

QUERY_RECORD_FIELDS_SQL = '''
with recursive
    includetree(name) as (
    select ? union all 
    select includes.include from includetree, includes 
    where includes.file_name = includetree.name)
select t.field, t.default_val from includetree,
(select records.file_name, records.field, records.default_val from records where records.record = ?) t 
where t.file_name = includetree.name;
'''

DEL_INCLUDE_SQL = '''
delete from includes where id in (select id from libs_info where parent_id = ?);
'''

DEL_DEFINE_SQL = '''
delete from defines where id in (select id from libs_info where parent_id = ?);
'''

DEL_RECORD_SQL = '''
delete from records where id in (select id from libs_info where parent_id = ?);
'''

class DataCache:
    def __init__(self, data_type = '', cache_dir = '', dir = None):
        self.dir = dir
        self.data_type = data_type
        self.cache_dir = cache_dir
        self.re_dict = GLOBAL_SET['compiled_re']
        self.pool_size = 8
        self.folder_id = 1
        if cache_dir != '':
            self.__init_db()

    def __init_db(self):
        if os.path.exists(self.cache_dir): 
            shutil.rmtree(self.cache_dir)
        self.db_con = sqlite3.connect(':memory:', check_same_thread = False)
        self.db_cur = self.db_con.cursor()
        self.db_cur.execute(CREATE_LIBS_INFO_SQL)
        self.db_cur.execute(CREATE_LIBS_SQL)
        self.db_cur.execute(CREATE_INCLUDE_SQL)
        self.db_cur.execute(CREATE_DEFINE_SQL)
        self.db_cur.execute(CREATE_RECORD_INFO_SQL)

    def query_mod_fun(self, module):
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_COMPLETION, (module, ))
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()

        completion_data = []
        all_fun = []
        for (fun_name, param_len, param_str) in query_data:
            if (fun_name, param_len) not in all_fun:
                param_list = self.format_param(param_str)
                completion = self.__tran2compeletion(fun_name, param_list, param_len)
                completion_data.append(['{}/{}\tMethod'.format(fun_name, param_len), completion])
                all_fun.append((fun_name, param_len))

        return completion_data

    def query_all_mod(self):
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_ALL_MOD)
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()

        completion_data = []
        for (mod_name, ) in query_data:
            completion_data.append(['{}\tModule'.format(mod_name), mod_name])

        return completion_data

    def query_fun_position(self, module, function):
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_POSITION, (module, function))
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()

        completion_data = []
        for (folder, fun_name, param_len, row_num) in query_data:
            filepath = os.path.join(folder, module + '.erl')
            completion_data.append(('{}/{}'.format(fun_name, param_len), filepath, row_num))

        return completion_data

    def query_file_defines(self, filepath):
        filename = self.get_filename_from_path(filepath)
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_DEFINE_SQL, (filename, ))
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()
        completion_data = []
        for (define, ) in query_data:
            completion_data.append([('{0}\tdefine').format(define), ('{0}${1}').format(define,1)])
        return completion_data

    def query_file_record(self, filepath):
        filename = self.get_filename_from_path(filepath)
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_RECORD_SQL, (filename, ))
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()
        completion_data = []
        for (record, ) in query_data:
            completion_data.append([('{0}\trecord').format(record), ('{0}${1}').format(record,1)])
        return completion_data

    def query_record_fields(self, filepath, record, need_show_equal):
        filename = self.get_filename_from_path(filepath)
        query_data = []
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_RECORD_FIELDS_SQL, (filename, record, ))
            query_data = self.db_cur.fetchall()
        finally:
            self.lock.release()
        completion_data = []
        for (field, default_val) in query_data:
            if need_show_equal:
                completion_data.append([('{0}\tfield').format(field), ('{0} = ${{{1}:{2}}}${3}').format(field, 1, default_val, 2)])
            else:    
                completion_data.append([('{0}\tfield').format(field), ('{0}').format(field)])
        return completion_data

    def build_module_index(self, filepath, folder_id):
        with open(filepath, encoding = 'UTF-8', errors='ignore') as fd:
            content = fd.read()
            code = re.sub(self.re_dict['comment'], '\n', content)

            export_fun = {}
            is_export_all = self.re_dict['export_all'].search(code)
            if not is_export_all:
                for export_match in self.re_dict['export'].finditer(code):
                    for funname_match in self.re_dict['funname'].finditer(export_match.group()):
                        [name, cnt] = funname_match.group().split('/')
                        export_fun[(name, int(cnt))] = None
            module = self.get_module_from_path(filepath)
            filename = self.get_filename_from_path(filepath)

            row_num = 1
            includes = []
            defines = []
            for line in code.split('\n'):
                funhead = self.re_dict['funline'].search(line)
                if funhead is not None: 
                    fun_name = funhead.group(1)
                    param_str = funhead.group(2)
                    param_list = self.format_param(param_str)
                    param_len = len(param_list)
                    if (fun_name, param_len) in export_fun or is_export_all != None:
                        if is_export_all == None:
                            del(export_fun[(fun_name, param_len)])
                        self.db_execute(INSERT_LIBS_SQL, (folder_id, module, fun_name, param_len, row_num, param_str))
                else:
                    includehead = self.re_dict['take_include'].search(line)
                    if includehead is not None:
                        includefile = includehead.group(1)
                        if includefile not in includes:
                            self.db_execute(INSERT_INCLUDE_INFO_SQL, (folder_id, filename, includefile))
                    else:
                        definehead = self.re_dict['defineline'].search(line)
                        if definehead is not None:
                            define = definehead.group(1)
                            if define not in defines:
                                self.db_execute(INSERT_DEFINE_SQL, (folder_id, filename, define))
                row_num += 1

            records = []
            records_match = self.re_dict['record_re'].findall(code)
            for (record, fields_data) in records_match:
                if record not in records:
                    records.append(record)
                    fields_match = self.re_dict['record_field_re'].findall(fields_data)
                    for (field, default_val) in fields_match:
                        self.db_execute(INSERT_RECORD_INFO_SQL,(folder_id, filename, record, field, default_val))

    def db_execute(self, sql, params):
        try:
            self.lock.acquire(True)
            self.db_cur.execute(sql, params)
        finally:
            self.lock.release()

    def get_module_from_path(self, filepath):
        (path, filename) = os.path.split(filepath)
        (module, extension) = os.path.splitext(filename)
        return module

    def get_filename_from_path(self, filepath):
        (path, filename) = os.path.split(filepath)
        return filename

    def format_param(self, param_str):
        param_str = re.sub(self.re_dict['special_param'], 'Param', param_str)
        param_str = re.sub(self.re_dict['='], '', param_str)

        if param_str == '' or re.match('\s+', param_str):
            return []
        else:
            return re.split(',\s*', param_str)

    def __tran2compeletion(self, funname, params, len):
        param_list = ['${{{0}:{1}}}'.format(i + 1, params[i]) for i in range(len)]
        param_str = ', '.join(param_list)
        completion = '{0}({1})${2}'.format(funname, param_str, len + 1)
        return completion

    def build_data(self):
        self.build_dir_data(self.dir)

    def build_dir_data(self, dirpath):
        all_filepath = []
        start_time = time.time()
        task_pool = ThreadPool(self.pool_size)
        self.lock = threading.Lock()

        if dirpath == None:
            folders = self.get_all_open_folders()
        else:
            folders = dirpath

        is_save_build_index = False
        for folder in folders:
            if self.get_folder_id(folder) != None:
                continue

            print('build {}: {} index'.format(self.data_type, folder))
            is_save_build_index = True
            self.db_cur.execute(INSERT_FOLDER_INFO, (self.folder_id, 0, folder))
            parent_id = self.folder_id
            for root, dirs, files in os.walk(folder):
                erl_files = fnmatch.filter(files, '*.[e|h]rl')
                if erl_files == []:
                    continue
                    
                if folder != root:
                    self.folder_id += 1
                    self.db_cur.execute(INSERT_FOLDER_INFO, (self.folder_id, parent_id, root))
                for file in erl_files:
                    all_filepath.append((os.path.join(root, file), self.folder_id))
                self.folder_id += 1
        
        task_pool.starmap(self.build_module_index, all_filepath)
        self.db_con.commit()
        is_save_build_index and print("build {} index, use {} second".format(self.data_type, time.time() - start_time))

    def get_all_open_folders(self):
        all_folders = []
        for window in sublime.windows():
            all_folders = all_folders + window.folders()

        return all_folders

    def get_folder_id(self, folder):
        result = None
        try:
            self.lock.acquire(True)
            self.db_cur.execute(QUERY_FOLDER, (folder, ))
            result = self.db_cur.fetchall()
        finally:
            self.lock.release()

        if result is None:
            return None
        for (fid, pid) in result:
            return (fid, pid)


    def rebuild_module_index(self, filepath):
        (folder, filename) = os.path.split(filepath)
        (module, extension) = os.path.splitext(filename)
        get_fid_return = self.get_folder_id(folder)
        if get_fid_return == None:
            self.build_dir_data([folder])
            return
        (fid, pid) = get_fid_return
        try:
            self.lock.acquire(True)
            self.db_cur.execute(DEL_LIBS_SQL, (fid, module))
        finally:
            self.lock.release()
        self.build_module_index(filepath, fid)
        self.db_con.commit()

    def delete_module_index(self, folders):
        for folder in folders:
            try:
                self.lock.acquire(True)
                folder_info = self.get_folder_id(folder)
                self.db_cur.execute(DEL_FOLDER_LIBS_SQL, (folder_info[0], ))
                self.db_cur.execute(DEL_INCLUDE_SQL, (folder_info[0], ))
                self.db_cur.execute(DEL_DEFINE_SQL, (folder_info[0], ))
                self.db_cur.execute(DEL_RECORD_SQL, (folder_info[0], ))
                self.db_cur.execute(DEL_FOLDER_SQL, (folder_info[0], folder))
            finally:
                self.lock.release()
        self.db_con.commit()

    def build_data_async(self):
        this = self
        class BuildDataAsync(threading.Thread):
            def run(self):
                this.build_data()
                
        BuildDataAsync().start()

    def looking_for_ther_nearest_record(self, view, pos):
        stack = []
        if pos - 2 > 0 and view.substr(pos - 1) == '.':
            record = []
            pos -= 1
            while pos > 0:
                pos -= 1
                char = view.substr(pos)
                if char == '#' and record != []:
                    record.reverse()
                    return record, False
                if char == ' ':
                    return [], False
                record.append(char)
        else:
            in_str = False
            found_first_spec_word = False
            while pos > 0:
                char = view.substr(pos)
                match_spec = re.compile(r'\w').match(char)
                if match_spec is None and found_first_spec_word == False :
                    found_first_spec_word = True
                    if char == '=':
                        return [], False
                if char == '"':
                    if len(stack) == 0 or stack[len(stack) - 1] != char:
                        in_str = True
                        stack.append(char)
                    elif stack[len(stack) - 1] == char:
                        in_str = False
                        stack.pop()
                if char == '}' and in_str == False:
                    stack.append(char)
                if char == '{' and in_str == False:
                    if len(stack) == 0:
                        record = []
                        while pos > 0:
                            pos -= 1
                            char = view.substr(pos)
                            if char == '#' and record != []:
                                record.reverse()
                                return record, True
                            if char == ' ':
                                break
                            record.append(char)
                    elif stack[len(stack) - 1] == '}':
                        stack.pop()
                    else:
                        return [], False
                pos -= 1
            return [], False
