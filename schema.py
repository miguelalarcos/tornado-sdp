import time
from api import current_user
from errors import SetError, ValidationError, PathError

now = lambda *args: time.time()
identity = lambda x: x
public = lambda *args: True
never  = lambda *args: False
hidden = lambda *args: False
private = hidden

def required(v):
    return v is not None

def context(id):
    def helper(ctx, doc):
        return ctx[id]
    return helper

def read_only(*args):
    return False

def default(v):
    def helper(ctx):
        return v
    return helper

def now(*args):
    return time.time()

def hidden(*args):
    return False

def is_owner(doc):
    return current_user() in doc['__owners']

def current_user_is(prop):
    def helper(root_doc, new_doc=None):
        if root_doc[prop] == current_user():
            return True
        else:
            return False
    return helper

class Schema:
    def __init__(self, schema):
        self.schema = schema
        self.kw = ['__get', '__set', '__set_document', '__get_document', \
                   '__create_document', '__ownership', '__set_default', \
                   '__get_default']

    def __getitem__(self, index):
        
        return self.schema[index]

    def get(self, document, root_doc=None):
        if root_doc is None:
            root_doc = document
        
        g = get_default = self.schema.get('__get_default', public)
        default_value = get_default(root_doc)
        get_default = lambda *args: default_value
        schema = self.schema
        #g = schema.get('__get', get_default)
        if not g(root_doc):
            return None
        ret = {}
        set_document = set(document.keys())
        set_schema = set(schema.keys()) - set(self.kw)
        intersection =  set_document & set_schema

        for key in intersection:
            g = schema[key].get('get', public)
            v = g(root_doc or document)
            
            if schema[key]['type'].__class__ == Schema:
                if not v:
                    ret[key] = None
                else:
                    ret[key] = schema[key]['type'].get(document[key], root_doc)
            elif schema[key]['type'].__class__ is list:
                if not v:
                    ret[key] = []
                else:
                    ret[key] = [schema[key]['type'][0].get(k, root_doc) for k in document[key]] 
            elif v:
                ret[key] = document[key]
        return ret

    def post(self,document, context=None, root_doc=None):
        
        if context is None:
            context = {}
        if root_doc is None:
            root_doc = document
        schema = self.schema

        c = schema.get('__create_document', lambda *args: True)
        if not c(root_doc):
            raise Exception('can not create document')

        ret = {}
        set_document = set(document.keys())
        set_schema = set(schema.keys()) - set(self.kw)
        intersection =  set_document & set_schema 
        missing = set_schema - set_document
        if len(set_document - set_schema) > 0:
            raise Exception('keywords not in schema')

        for key in missing | intersection:
            
            if schema[key]['type'].__class__ == Schema:
                
                if document.get(key):
                    
                    ret[key] = schema[key]['type'].post(document[key], context, root_doc)
            elif type(schema[key]['type']) is list:
                #schema = schema[key]['type'][0]
                if document.get(key):
                    ret[key] = [schema[key]['type'][0].post(k, context, root_doc) for k in document[key]]
            elif 'computed' not in schema[key]:
                validation = schema[key].get('validation', public)
                required = schema[key].get('required', False)
                mtype = schema[key]['type']
                initial = schema[key].get('initial')
                initial = initial and initial(root_doc) #initial(context)
                v = document.get(key, initial)
                
                if required and v is None:
                    raise ValidationError('required')
                ##if v is not None and (not type(v) is mtype or not validation(v)):
                if v is not None and (not isinstance(v, mtype) or not validation(v)):
                    raise ValidationError('not valid prop or missing', key)
                if key in intersection or initial is not None: 
                    ret[key] = document.get(key, initial)
            else:
                create = schema[key].get('computed')
                val = create(document) 
                ret[key] = val
        return ret

    def put(self, path, doc, value, type_='$set'):
        pull = False
        push = False
        if type_ == '$pull':
            pull = True
        elif type_ == '$push' or type_ == '$addToSet':
            push = True
        root_doc = doc
        schema = self.schema
        s = schema.get('__set_document', never)
        if not s(doc):
            raise SetError('no se puede setear, __set_document')
        
        set_default = schema.get('__set_default', never)
        default_value = set_default(root_doc)
        set_default = lambda *args: default_value
        validation = public
        computed = None

        paths = path.split('.')
        last = paths[-1]
        
        for key in paths:
            if key.isdigit():
                key = int(key)
                schema = schema[0]
                if schema.__class__ == Schema and not to_set(root_doc):
                    raise SetError('no se puede setear, set')
            else:
                try:
                    schema[key]
                except KeyError:
                    raise PathError('path does not exist', key)
                validation = schema[key].get('validation', validation)
                computed = schema[key].get('computed')
                to_set = schema[key].get('set', set_default)
                can_pull = schema[key].get('pull', never)
                can_push = schema[key].get('push', never)
                schema = schema[key]['type']
                
            if not pull and not push and not to_set(root_doc):
                raise SetError('no se puede setear, set')   
            if (schema.__class__ == Schema or type(schema) is list) and key != last:
                try:                
                    doc = doc[key]
                except KeyError:
                    raise PathError('path does not exist')  
        
        flag_list = False
        if schema is list or type(schema) is list:
            flag_list = True
            if pull and can_pull(doc, root_doc):
                return value
            elif push and not can_push(doc, root_doc):
                raise SetError('no se puede setear, push')
            elif pull:
                raise SetError('no se puede setear, pull')
            if type(schema) is list:
                schema = schema[0]
        
        if schema.__class__ == Schema:
            set_default = schema.schema.get('__set_default', never)
            keys = [k  for k in schema.schema.keys() if k not in self.kw] #  ['__set_document']]
            for k in keys:
                try:
                    schema[k]
                    if k not in value and 'initial' in schema[k]:
                        value[k] = schema[k]['initial'](root_doc)
                        sett = public
                    else:
                        sett = schema[k].get('set', set_default)
                    value[k]
                except KeyError:
                    raise PathError('keyerror path does not exist', k, schema.schema, value)
                except TypeError:
                    raise PathError('type error path does not exist', k)
                
                if not sett(root_doc): 
                    raise SetError('no se puede setear, set')
                if 'computed' in schema[k]:
                    value[k] = schema[k]['computed'](value)    
                if not schema[k]['type'] == type(value[k]) and not schema[k].get('validation', public)(value[k]):
                    raise ValidationError('no se puede setear, validation')
            return value
        else:
            if push and flag_list: # and can_push(doc, root_doc):
                return value  
            if not to_set(root_doc):
                raise SetError('no se puede setear, set')
            if computed is not None:
                value = computed(value) 
            
            if isinstance(value, schema) and validation(value):
                return value
            else:
                raise ValidationError('no se puede setear, validation', value)

