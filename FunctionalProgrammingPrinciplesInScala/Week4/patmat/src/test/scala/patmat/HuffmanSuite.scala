package patmat

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import patmat.Huffman._

@RunWith(classOf[JUnitRunner])
class HuffmanSuite extends FunSuite {
	trait TestTrees {
		val t1 = Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5)
		val t2 = Fork(Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5), Leaf('d',4), List('a','b','d'), 9)
	}


  test("weight of a larger tree") {
    new TestTrees {
      assert(weight(t1) === 5)
    }
  }


  test("chars of a larger tree") {
    new TestTrees {
      assert(chars(t2) === List('a','b','d'))
    }
  }


  test("string2chars(\"hello, world\")") {
    assert(string2Chars("hello, world") === List('h', 'e', 'l', 'l', 'o', ',', ' ', 'w', 'o', 'r', 'l', 'd'))
  }


  test("makeOrderedLeafList for some frequency table") {
    assert(makeOrderedLeafList(List(('t', 2), ('e', 1), ('x', 3))) === List(Leaf('e',1), Leaf('t',2), Leaf('x',3)))
  }


  test("combine of some leaf list") {
    val leaflist = List(Leaf('e', 1), Leaf('t', 2), Leaf('x', 4))
    assert(combine(leaflist) === List(Fork(Leaf('e',1),Leaf('t',2),List('e', 't'),3), Leaf('x',4)))

    assert(combine(Nil) === Nil)
    val singleTree = List(Leaf('e', 1))
    assert(combine(singleTree) === singleTree)
  }

  test("createCodeTree") {
    val codeTree = createCodeTree(string2Chars("ettxxxx"))
    assert(weight(codeTree) == 7)
    assert(chars(codeTree) == List('e', 't', 'x'))
  }

  test("decode") {
    val codeTree = createCodeTree(string2Chars("ettxxxx"))
    assert(decode(codeTree, List(0, 1, 0, 1, 1)) === string2Chars("ttx"))
    assert(decode(codeTree, List(1, 0, 1, 0, 0, 1, 0, 0, 0, 1)) === string2Chars("xtexet"))
    assert(decodedSecret === string2Chars("huffmanestcool"))
  }

  test("encode") {
    val codeTree = createCodeTree(string2Chars("ettxxxx"))
    assert(encode(codeTree)(string2Chars("ttx")) ===
      List(0, 1, 0, 1, 1))
    assert(encode(codeTree)(string2Chars("xtexet")) ===
      List(1, 0, 1, 0, 0, 1, 0, 0, 0, 1))
    new TestTrees {
      assert(decode(t2, encode(t2)("abdabddba".toList)) === "abdabddba".toList)
    }
  }

  test("convert") {
    val codeTree = createCodeTree(string2Chars("ettxxxx"))
    val codeTable = convert(codeTree)
    assert(codeTable === List(('e', List(0, 0)), ('t', List(0, 1)), ('x', List(1))))
    assert(codeBits(codeTable)('e') === List(0, 0))
    assert(codeBits(codeTable)('t') === List(0, 1))
    assert(codeBits(codeTable)('x') === List(1))
  }

  test("decode and encode a very short text should be identity") {
    new TestTrees {
      assert(decode(t1, encode(t1)("ab".toList)) === "ab".toList)
    }
  }

  test("quickencode") {
    val codeTree = createCodeTree(string2Chars("ettxxxx"))
    assert(quickEncode(codeTree)(string2Chars("ttx")) ===
      List(0, 1, 0, 1, 1))
    assert(quickEncode(codeTree)(string2Chars("xtexet")) ===
      List(1, 0, 1, 0, 0, 1, 0, 0, 0, 1))
    new TestTrees {
      assert(decode(t2, quickEncode(t2)("abdabddba".toList)) === "abdabddba".toList)
    }
  }
}
