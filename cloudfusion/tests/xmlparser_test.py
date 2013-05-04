'''
Created on 06.05.2011

@author: joe

'''

from cloudfusion.util.xmlparser import DictXMLParser

XML_STRING1 = '<collectionContents start="0" hasMore="false" end="4"><collection type="folder"><displayName>Sample Documents</displayName><ref>https://api.sugarsync.com/folder/:sc:1237636:40_267018165</ref><contents>https://api.sugarsync.com/folder/:sc:1237636:40_267018165/contents</contents></collection><collection type="folder"><displayName>Sample Music</displayName><ref>https://api.sugarsync.com/folder/:sc:1237636:40_267018582</ref><contents>https://api.sugarsync.com/folder/:sc:1237636:40_267018582/contents</contents></collection><collection type="folder"><displayName>Sample Photos</displayName><ref>https://api.sugarsync.com/folder/:sc:1237636:40_267018675</ref><contents>https://api.sugarsync.com/folder/:sc:1237636:40_267018675/contents</contents></collection><collection type="folder"><displayName>subFolder</displayName><ref>https://api.sugarsync.com/folder/:sc:1237636:40_279687768</ref><contents>https://api.sugarsync.com/folder/:sc:1237636:40_279687768/contents</contents></collection><collection type="folder"><displayName>subFolder2</displayName><ref>https://api.sugarsync.com/folder/:sc:1237636:40_279727285</ref><contents>https://api.sugarsync.com/folder/:sc:1237636:40_279727285/contents</contents></collection></collectionContents>'
simple_dict = {"collectionContents": {"collection": {"ref": ""}}}
dict_with_collection1 = {"collectionContents": {"[collection]": {"ref": ""}}}
XML_STRING2 = '<root><element>text1</element><element>text2</element></root>'
dict_with_collection2 = {"root": {"[element]": "element"}}

def test_populate_dict_with_XML_leaf_textnodes():
    DictXMLParser().populate_dict_with_XML_leaf_textnodes(XML_STRING1, simple_dict)
    assert simple_dict["collectionContents"]["collection"]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_267018165"
     
def test_populate_dict_with_XML_collection_leaf_textnodes2():
    DictXMLParser().populate_dict_with_XML_collection_leaf_textnodes(XML_STRING2, dict_with_collection2)
    assert dict_with_collection2["root"]["[element]"][0] == "text1"
    assert dict_with_collection2["root"]["[element]"][1] == "text2"
     
def test_populate_dict_with_XML_collection_leaf_textnodes1():
    DictXMLParser().populate_dict_with_XML_collection_leaf_textnodes(XML_STRING1, dict_with_collection1)
    assert dict_with_collection1["collectionContents"]["[collection]"][0]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_267018165"
    assert dict_with_collection1["collectionContents"]["[collection]"][1]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_267018582"
    assert dict_with_collection1["collectionContents"]["[collection]"][2]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_267018675"
    assert dict_with_collection1["collectionContents"]["[collection]"][3]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_279687768"
    assert dict_with_collection1["collectionContents"]["[collection]"][4]["ref"] == "https://api.sugarsync.com/folder/:sc:1237636:40_279727285"
