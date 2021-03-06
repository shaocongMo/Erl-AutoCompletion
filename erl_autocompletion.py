from .util import *
from functools import partial
import sublime_plugin, sublime, re, os, sys, shutil

cache = {}

ERL_AUTO_COMPLETE = ['#', '.', '{', '?', ':']

def plugin_loaded():
    global cache

    cache_dir = os.path.join(sublime.cache_path(), GLOBAL_SET['package_name'])
    cache['libs'] = DataCache('libs', cache_dir, [get_erl_lib_dir()])
    cache['libs'].build_data_async()

    cache['project'] = DataCache('project', cache_dir)
    cache['project'].build_data_async()

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

        view_sel = view.sel()
        sel = view_sel[0]
        pos = sel.end()
        point = locations[0] - len(prefix) - 1
        if view.substr(pos - 1) in ERL_AUTO_COMPLETE:
            field_pos = pos
            letter = view.substr(pos - 1)
        else:
            field_pos = locations[0] - 1
            letter = view.substr(point)

        if letter == ':':
            # show function
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
        elif letter == '?':
            # show define list
            filepath = view.file_name()
            completion = cache['project'].query_file_defines(filepath)
            if completion != []:
                return (completion, sublime.INHIBIT_WORD_COMPLETIONS | sublime.INHIBIT_EXPLICIT_COMPLETIONS)
            return
        elif letter == '#':
            # show record list
            filepath = view.file_name()
            completion = cache['project'].query_file_record(filepath)
            if completion != []:
                return (completion, sublime.INHIBIT_WORD_COMPLETIONS | sublime.INHIBIT_EXPLICIT_COMPLETIONS)
            return
        else:
            if letter == '-' and view.substr(view.line(point))[0] == '-':
                return GLOBAL_SET['-key']

            (record, need_show_equal) = cache['project'].looking_for_ther_nearest_record(view, field_pos)
            if record != []:
                # show record field
                record_name = "".join(record)
                fields = cache['project'].query_record_fields(view.file_name(), record_name, need_show_equal)
                if fields != []:
                    return (fields, sublime.INHIBIT_WORD_COMPLETIONS | sublime.INHIBIT_EXPLICIT_COMPLETIONS)

            if re.match('^[0-9a-z_]+$', prefix) and len(prefix) > 1:
                # show module
                modules = cache['libs'].query_all_mod() + cache['project'].query_all_mod() + cache['libs'].query_mod_fun('erlang')
                return [ (mname, mval+":") for (mname, mval) in modules]
            
            # return None

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

        cache['project'].rebuild_module_index(view.file_name())

    def on_window_command(self, window, command_name, args):
        if command_name == 'remove_folder':
            cache['project'].delete_module_index(args['dirs'])

    def on_load(self, view):
        cache['project'].build_data_async()

    def on_modified(self, view):
        view_sel = view.sel()
        sel = view_sel[0]
        pos = sel.end()
        if not view.match_selector(pos, "source.erlang"): 
            return
        if view.substr(pos - 1) in ERL_AUTO_COMPLETE:
            view.run_command('auto_complete')

class GotoCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        return