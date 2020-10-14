from .util import *
from functools import partial
import sublime_plugin, sublime, re, os, sys, shutil, time

cache = {}

def plugin_loaded():
    global cache

    cache_dir = os.path.join(sublime.cache_path(), GLOBAL_SET['package_name'])
    cache['libs'] = DataCache('libs', cache_dir, [get_erl_lib_dir()])
    cache['libs'].build_data_async()

    cache['project'] = DataCache('project', cache_dir)
    cache['project'].build_data_async()
    cache['xrefConfig'] = False

def plugin_unloaded():
    from package_control import events

    package_name = GLOBAL_SET['package_name']
    if events.remove(package_name):
        print('remove {0}'.format(package_name))
        cache_dir = os.path.join(sublime.cache_path(), package_name)
        shutil.rmtree(cache_dir)

if sys.version_info < (3,):
    plugin_loaded()
    unload_handler = plugin_unloaded

class ErlListener(sublime_plugin.EventListener):
    def on_query_completions(self, view, prefix, locations):
        if not view.match_selector(locations[0], "source.erlang"): 
            return []

        point = locations[0] - len(prefix) - 1
        letter = view.substr(point)

        if letter == ':':
            module_name = view.substr(view.word(point))
            if module_name.strip() == ':': 
                return

            flag = sublime.INHIBIT_WORD_COMPLETIONS | sublime.INHIBIT_EXPLICIT_COMPLETIONS
            completion = cache['libs'].query_mod_fun(module_name)
            if completion != []:
                return (completion, flag)
            completion = cache['project'].query_mod_fun(module_name)
            if completion != []:
                return (completion, flag)
        else:
            if letter == '-' and view.substr(view.line(point))[0] == '-':
                return GLOBAL_SET['-key']

            if re.match('^[0-9a-z_]+$', prefix) and len(prefix) > 1:
                return cache['libs'].query_all_mod() + cache['project'].query_all_mod() + cache['libs'].query_mod_fun('erlang')
            
            return ([], sublime.INHIBIT_EXPLICIT_COMPLETIONS)

    def on_text_command(self, view, command_name, args):
        if command_name == 'goto':
            if args and 'event' in args:
                event = args['event']
                point = view.window_to_text((event['x'], event['y']))
            else:
                sels = view.sel()
                point = sels[0].begin()

            if not view.match_selector(point, "source.erlang"): 
                return

            go_to = GoTo()
            go_to.run(point, view, cache, is_quick_panel = True)

    def on_hover(self, view, point, hover_zone):
        if not view.match_selector(point, "source.erlang"): 
            return

        go_to = GoTo()
        go_to.run(point, view, cache)

    def on_post_save_async(self, view):
        caret = view.sel()[0].a

        if not ('source.erlang' in view.scope_name(caret)): 
            return

        auto_compile(view)

        cache['project'].rebuild_module_index(view.file_name())

    def on_window_command(self, window, command_name, args):
        if command_name == 'remove_folder':
            cache['project'].delete_module_index(args['dirs'])

    def on_load(self, view):
        cache['project'].build_data_async()

class GotoCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        return

class ErlangCompileShowCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        id = 'erlang_compile_show'
        w = self.view.window().get_output_panel(id)
        if not w:
            w = self.view.window().create_output_panel(id)
        self.view.window().run_command("show_panel", {"panel": ("output.%s" % id)})
        w.run_command('erlang_compile_show_panel')
        return

class ErlangCompileShowPanelCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        self.view.insert(edit, 0, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n' + cache['compile_log'])
        return

def auto_compile(view):
    setting = sublime.load_settings('erl_autocompletion.sublime-settings')
    auto_compile = setting.get('erl_auto_compile', False)
    folders = view.window().folders()
    if len(folders) > 0 and auto_compile :
        root = folders[0] + '/'
        file_name = view.file_name()
        erlc_path = setting.get('erlc_path', 'erlc')
        cmd = erlc_path
        output_path = root + setting.get('erl_output_path', './ebin')
        cmd = cmd + ' -o ' + output_path
        for include_path in setting.get('erl_include_path', ['./include']):
            cmd = cmd + ' -I ' + root + include_path
        xref_check = xrefCheck(view, setting, erlc_path, root + include_path, file_name, output_path)
        cmd = cmd + ' ' + file_name
        with os.popen(cmd) as f:
            data = f.read()
        if len(xref_check) > 0 :
            cache['compile_log'] = xref_check
        else:
            if len(data) > 0 :
                cache['compile_log'] = 'Compile:\n' + data
            else :
                cache['compile_log'] = file_name + ' compile success'
        view.run_command('erlang_compile_show')

def xrefCheck(view, setting, erlc_path, include_path, file_name, output_path):
    xref_check = setting.get('xref_check', False)
    if xref_check :
        buildXrefConfig(view, setting)
        cmd = erlc_path + ' +debug_info -I ' + include_path + ' ' + file_name
        with os.popen(cmd) as f:
            data = f.read()
        with os.popen('xrefr') as f:
            xrefData = f.read()
            if len(xrefData) > 0:
                if len(data) > 0:
                    data = 'Compile:\n' + data + '\nXref:\n' + xrefData
                else:
                    data = 'Xref:\n' + xrefData
            else:
                if len(data) > 0:
                    data = 'Compile:\n' + data
        beamPath = sublime.packages_path() + '\\Erl-AutoCompletion\\util\\' + file_name.split('\\')[-1][:-3] + 'beam'
        if os.path.exists(beamPath):
            os.remove(beamPath)
        return data
    return ''

def buildXrefConfig(view, setting):
    folders = view.window().folders()
    root = folders[0] + '/'
    if cache['xrefConfig'] != root:
        cache['xrefConfig'] = root
        output_path = root + setting.get('erl_output_path', './ebin')
        configPath = sublime.packages_path() + '\\Erl-AutoCompletion\\util\\xref.config'
        configData = '[{xref,[{config,#{dirs=>["./"],extra_paths=>["' + output_path.replace('\\', '/') + '"]}},{checks,[undefined_function_calls]}]}].'
        configFile = open(configPath, 'w')
        configFile.write(configData)
        configFile.flush()
        configFile.close()