import importlib.util,json,tempfile,tracemalloc,unittest
from pathlib import Path
from unittest.mock import patch

class SaveTests(unittest.TestCase):
 def test_both_scripts(self):
  for name in ('collect_football','collect_hockey'):
   spec=importlib.util.spec_from_file_location(name,Path(__file__).parent/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
   with tempfile.TemporaryDirectory() as temp:
    path=Path(temp)/'large.json';data={'items':['я'*4096]*16384}
    tracemalloc.start()
    with patch.object(m.json,'dumps',side_effect=AssertionError('whole-object dumps forbidden')):m.write(path,data)
    _,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert path.stat().st_size>128*1024*1024
    assert peak<8*1024*1024,peak
    print(name,'128 MiB JSON; extra traced Python memory:',round(peak/1024/1024,2),'MiB')
    m.write(path,{'old':True})
    with self.assertRaises(ValueError):m.write(path,{'bad':float('nan')})
    self.assertEqual(json.loads(path.read_text()),{'old':True});self.assertEqual(list(Path(temp).glob('*.tmp')),[])
    with patch.object(m.os,'replace',side_effect=OSError(28,'No space left on device')):
     with self.assertRaises(OSError):m.write(path,{'new':True})
    self.assertEqual(json.loads(path.read_text()),{'old':True})
    obj={'secret-key':['xsecret-key',{'nested':'secret-key'}]};self.assertIs(m.redact_in_place(obj,'secret-key'),obj)
    self.assertNotIn('secret-key',json.dumps(obj))
if __name__=='__main__':unittest.main()
