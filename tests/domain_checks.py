from workbench.domain import Once,typed_key,row_bytes,synthetic,stream_jsonl
def check(c,root,read):
    stream=Once(synthetic(3,'L'));c.assertEqual(len(list(stream)),3)
    with c.assertRaises(RuntimeError):list(stream)
    c.assertEqual(typed_key({'x':None},{'x':'integer'}),(None,))
    with c.assertRaises(ValueError):typed_key({'x':True},{'x':'integer'})
    c.assertGreater(row_bytes({'x':'甲'}),64)
    c.assertEqual(len(list(stream_jsonl(root/'samples/left.jsonl'))),4)
