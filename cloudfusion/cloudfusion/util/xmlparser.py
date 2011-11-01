"""
Parses an XML file or string, populating a dictionary containing the keys with the desired values. 

The keys of the dictionary are either the names of Tags contained in the XML data or again dictionaries.
The values can either be dictionaries again or simple strings, but ultimately the leaf values will be  

"""

import xml.dom.minidom 
from copy import deepcopy

class DictXMLParser(object):

    def getText(self, node):
        rc = []
        for node in node.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def __populate_dict(self, dom_tree, dict_tree):
        """  Does the work for :meth:`populate_dict_with_XML_leaf_textnodes`
        :param dom_tree: An XML element
        :param dict_tree: A dictionary of keys/values matching the first XML subtree in :param:`dom_tree`
        """
        for enclosing_tag, enclosed_tag in dict_tree.iteritems():
            if type(enclosed_tag) == dict:
                dom_tree = self.get_elements_by_tag_name(dom_tree, enclosing_tag)[0]
                self.__populate_dict(dom_tree, enclosed_tag)
            else:
                leaf = self.get_elements_by_tag_name(dom_tree, enclosing_tag)[0]
                dict_tree[enclosing_tag] = self.getText(leaf)

    def populate_dict_with_XML_leaf_textnodes(self, xml_data, dict_tree):
        """ Puts the text of the XML tags of :param:`xml_data` specified by :param:`dict_tree` into :param:`dict_tree` .
        
        The :param:`dict_tree` is a dictionary describing a subtree of :param:`xml_data`. 
        The dictionarie's keys correspond to the first XML tags with the same name on the same nesting level of the dictionary.
        Therefore the keys in the dictionary {"root": {"element1": "", "element2": ""}} match the root tag <root> 
        as well as the subelements of root <element1> and <element2>.
        The dictionarie's most deeply nested elements must be empty strings. 
        This value is replaced by the text of the corresponding key's XML tag.
        Given the previous dictionary and the following XML structure:
        .. code-block:: xml
        
            <root>
                <element1>text1</element1>
                <element2>text2</element2>
            </root>
        
        The resulting dictionary would be {"root": {"element1": "text1", "element2": "text2"}}        
           
        :param xml_data: An XML string or an XML file
        :param dict_tree: A dictionary of keys/values matching the first XML subtree in :param:`xml_tree`
        """
        if type(xml_data) == str:
            dom_tree = xml.dom.minidom.parseString(xml_data)
        else:
            dom_tree = xml.dom.minidom.parse(xml_data)
        self.__populate_dict(dom_tree, dict_tree)
        
    def populate_dict_with_XML_collection_leaf_textnodes(self, xml_tree, dict_tree):
        """ Puts the text of the XML tags of :param:`xml_tree` specified by :param:`dict_tree` into :param:`dict_tree` .
        
        The :param:`dict_tree` is a dictionary describing a subtree of :param:`xml_data`. 
        The dictionarie's keys correspond to the first XML tags with the same name on the same nesting level of the dictionary.
        Additionally, keys with the name surrounded by brackets are seen as a list of such XML tags.
        Therefore the keys in the dictionary {"root": {"[element]": ""}} match the root tag <root> 
        as well as the subelements of root <element> and <element>.
        The dictionarie's most deeply nested elements must be empty strings. 
        This value is replaced by the text of the corresponding key's XML tag.
        Given the previous dictionary and the following XML structure:
        .. code-block:: xml
        
            <root>
                <element>text1</element>
                <element>text2</element>
            </root>
        
        The resulting dictionary would be {"root": {"[element]": ["text1", "text2"]}}        
           
        :param xml_tree: An XML string or an XML file
        :param dict_tree: A dictionary of keys/values matching the first XML subtree in :param:`xml_tree`
        """
        if type(xml_tree) == str:
            dom_tree = xml.dom.minidom.parseString(xml_tree)
        else:
            dom_tree = xml.dom.minidom.parse(xml_tree)
        self.__populate_dict_with_collections(dom_tree, dict_tree)
        
    def get_elements_by_tag_name(self, dom_tree, tag_name):
        """:returns: elem a list of elements matching the tags with the name :param:`tag_name` on the first level of  :param:`dom_tree`."""
        ret = []
        for subtree in dom_tree.childNodes:
            if subtree.localName==tag_name:
                ret.append(subtree)
        return ret
    
    def __populate_dict_with_collections(self, dom_tree, dict_tree):
        """  Does the work for :meth:`populate_dict_with_XML_collection_leaf_textnodes`
        :param dom_tree: An XML element
        :param dict_tree: A dictionary of keys/values matching the first XML subtree in :param:`dom_tree`
        """
            
        for enclosing_tag, enclosed_tag in dict_tree.iteritems():
            #print enclosing_tag, enclosed_tag
            #print           dom_tree.getElementsByTagName(enclosing_tag)
            #print           dom_tree.getElementsByTagName(enclosing_tag[1:-1])
            if enclosing_tag.startswith("["): # handle as collection
                elem_collection = []
                for collection_elem in self.get_elements_by_tag_name( dom_tree, enclosing_tag[1:-1] ):
                    if type(enclosed_tag) != dict:
                        elem_collection.append( self.getText(collection_elem) )
                    else:
                        enclosed_tag_copy = deepcopy(enclosed_tag)
                        self.__populate_dict_with_collections( collection_elem, enclosed_tag_copy )
                        elem_collection.append(enclosed_tag_copy)
                dict_tree[enclosing_tag] = elem_collection
            elif type(enclosed_tag) == dict:
                dom_tree = self.get_elements_by_tag_name(dom_tree, enclosing_tag)[0]
                self.__populate_dict_with_collections(dom_tree, enclosed_tag)
            else:
                leaf = self.get_elements_by_tag_name(dom_tree, enclosing_tag)[0]
                dict_tree[enclosing_tag] = self.getText(leaf)
        