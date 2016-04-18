/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off
// TODO: fix

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{ExprCode, CodegenContext}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import scala.collection.mutable

import javassist.{Modifier, ClassPool, CtMethod}
import javassist.bytecode.Opcode._
import javassist.bytecode.{ConstPool, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  def analyzeMethod(
      method: CtMethod,
      schema: StructType,
      children: Seq[Expression],
      initExprs: Map[String, Expression] = Map.empty,
      stackHeightAtEntry: Int = 0,
      pos: Int = 0): Option[Expression] = {
    println("-" * 80)
    println(method.getName)
    println(children)
    println("-" * 80)

    // Expression map: key is stack value name. value is the Catalyst expression used to
    // create the stack value.
    val exprs = mutable.Map[String, Expression]()
    exprs ++= initExprs

    // Emulate stack variables. when given a stack height $i, it returns the corresponding variable
    // [[stack$i]].
    def stack(i: Int) = s"stack$i"

    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    // Move the point of current instruction to given position (default is zero).
    instructions.move(pos)
    val constPool = method.getMethodInfo.getConstPool

    {
      val i = method.getMethodInfo().getCodeAttribute.iterator()
      while (i.hasNext) {
        println(InstructionPrinter.instructionString(i, i.next(), constPool))
      }
    }

    def ldc(pos: Int): Any = _ldc(pos, instructions.byteAt(pos + 1))
    def ldcw(pos: Int): Any = _ldc(pos, instructions.u16bitAt(pos + 1))
    def _ldc(pos: Int, cp_index: Int): Any = {
      constPool.getTag(cp_index) match {
        case ConstPool.CONST_Double => constPool.getDoubleInfo(cp_index)
        case ConstPool.CONST_Float => constPool.getFloatInfo(cp_index)
        case ConstPool.CONST_Integer => constPool.getIntegerInfo(cp_index)
        case ConstPool.CONST_Long => constPool.getLongInfo(cp_index)
        case ConstPool.CONST_String => constPool.getStringInfo(cp_index)
      }
    }

    def getInvokeMethodTarget(pos: Int) = {
      val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    def getInvokeInterfaceTarget(pos: Int) = {
      val mrClassName = constPool.getInterfaceMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getInterfaceMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getInterfaceMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    var stackHeight = stackHeightAtEntry

    while (instructions.hasNext) {
      // Fetch next op code
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      val mnemonic = InstructionPrinter.instructionString(instructions, pos, constPool)
      println("*" * 20 + " " + mnemonic + s" (stack = $stackHeight) " + "*" * 20)

      // How the stack will grow after this op code:
      // For example, I2L (convert an int into a long) will pop a value from stack and push
      // its result into it. So the grown number is 0.
      val stackGrow: Int = {
        op match {
          case INVOKEVIRTUAL => -1 * getInvokeMethodTarget(pos).getParameterTypes.length
          case INVOKESTATIC => -1 * getInvokeMethodTarget(pos).getParameterTypes.length
          case INVOKEINTERFACE => -1 * getInvokeInterfaceTarget(pos).getParameterTypes.length
          case LDC2_W => 1 // TODO: in reality, this pushes 2; this is a hack.
          case GETFIELD => 1 // hack
          case LCONST_0 | LCONST_1 => 1 // hack
          case LADD | DADD | LMUL | DMUL => -1
          case I2B | I2C | I2F | I2S | I2L | I2D => 0 // hack
          case _ => STACK_GROW(op) // pre-defined stack grow in javassist
        }
      }

      // The value in the Stack which is the result of current op code.
      // We can use this name to get corresponding Catalyst expression from expression map.
      val targetVarName = stack(stackHeight + stackGrow)

      // The Catalyst expression used to create the value at the top - 1 position of stack.
      def stackHeadMinus1 = exprs(stack(stackHeight - 1))
      // The Catalyst expression used to create the value at the top of stack.
      def stackHead = exprs(stack(stackHeight))

      def analyzeCMP(
          value1: Expression,
          value2: Expression,
          compOp: (Expression, Expression) => Predicate): Option[Expression] = {
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          trueJumpTarget)
        val falseExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          instructions.next())
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(value1, value2), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      def analyzeIFNull(
          compOp: (Expression) => Predicate): Option[Expression] = {
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          trueJumpTarget)
        val falseExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          instructions.next())
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(stackHead), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      exprs(targetVarName) = op match {
        // load a reference onto the stack from local variable {0, 1, 2}
        case ALOAD_0 => children(0)
        case ALOAD_1 => children(1)
        case ALOAD_2 => children(2)

        // push a byte onto the stack as an integer value
        case BIPUSH => Literal(instructions.byteAt(pos + 1)) // TODO: byte must be sign-extended into an integer value?
        // load the int value {0, 1, ..} onto the stack
        case ICONST_0 => Literal(0)
        case ICONST_1 => Literal(1)
        case ICONST_2 => Literal(2)
        case ICONST_3 => Literal(3)
        case ICONST_4 => Literal(4)

        // push the long {0, 1} onto the stack
        case LCONST_0 => Literal(0L)
        case LCONST_1 => Literal(1L)

        // load an int value from local variable {0, 1, 2}
        case ILOAD_0 => children(0)
        case ILOAD_1 => children(1)
        case ILOAD_2 => children(2)

        // goes to another instruction at branchoffset: byte at [pos + 1] << 8 + byte at [pos + 2].
        // we directly fetch 2 bytes here.
        case GOTO =>
          val target = instructions.s16bitAt(pos + 1) + pos
          return analyzeMethod(method, schema, children, exprs.toMap, stackHeight, target)

        // convert an int into a byte
        case I2B => Cast(stackHead, ByteType)
        // convert an int into a character
        case I2C => Cast(stackHead, StringType)
        // convert an int into a double
        case I2D => Cast(stackHead, DoubleType)
        // convert an int into a float
        case I2F => Cast(stackHead, FloatType)
        // convert an int into a long
        case I2L => Cast(stackHead, LongType)
        // convert an int into a short
        case I2S => Cast(stackHead, ShortType)

        // multiply two integers/longs
        case IMUL | DMUL | LMUL => Multiply(stackHeadMinus1, stackHead)

        // add two integers/longs/doubles
        case IADD | DADD | LADD => Add(stackHeadMinus1, stackHead)

        // push a constant at #index = [pos + 1] from a constant pool
        // (string, int or float) into stack
        case LDC => Literal(ldc(pos))
        // push a constant at #index = [pos + 1] << 8 + [pos + 2] from a constant pool
        // (string, int or float) onto the stack
        case LDC_W => Literal(ldcw(pos))
        // push a constant at #index = [pos + 1] << 8 + [pos + 2] from a constant pool
        // (double or long) onto the stack
        // In JVM, this pushes two words onto the stack, but we're going to only push one.
        case LDC2_W => Literal(ldcw(pos))

        // if stackHeadMinus1 is less than or equal to stackHead,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPLE => return analyzeCMP(stackHeadMinus1, stackHead, LessThanOrEqual)
        // if stackHeadMinus1 and stackHead are not equal,
        // branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        case IF_ICMPNE =>
          return analyzeCMP(stackHeadMinus1, stackHead, (e1, e2) => Not(EqualTo(e1, e2)))

        // if stackHead is not 0, branch to instruction at branchoffset = [pos + 1] << 8 + [pos + 2]
        // the foillowing branching ops follow the same sematics.
        case IFNE => return analyzeCMP(stackHead, Literal(0), (e1, e2) => Not(EqualTo(e1, e2)))
        case IFGT => return analyzeCMP(stackHead, Literal(0), (e1, e2) => GreaterThan(e1, e2))
        case IFGE =>
          return analyzeCMP(stackHead, Literal(0), (e1, e2) => GreaterThanOrEqual(e1, e2))
        case IFLT => return analyzeCMP(stackHead, Literal(0), (e1, e2) => LessThan(e1, e2))
        case IFLE => return analyzeCMP(stackHead, Literal(0), (e1, e2) => LessThanOrEqual(e1, e2))
        case IFEQ => return analyzeCMP(stackHead, Literal(0), (e1, e2) => EqualTo(e1, e2))
        case IFNONNULL => return analyzeIFNull((e) => IsNull(e))
        case IFNULL => return analyzeIFNull((e) => IsNotNull(e))

        case CHECKCAST =>
          val cp_index = instructions.u16bitAt(pos + 1)
          val className = constPool.getClassInfo(cp_index)
          CheckCast(stackHead, Literal(className))

        case INVOKEINTERFACE =>
          val target = getInvokeInterfaceTarget(pos)
          if (target.getDeclaringClass.getName == classOf[Row].getName) {
            if (target.getName.startsWith("get")) {
              val fieldNumber = stackHead.asInstanceOf[Literal].value.asInstanceOf[Int]
              NPEOnNull(UnresolvedAttribute(schema.fields(fieldNumber).name))
            } else if (target.getName == "isNullAt") {
              val fieldNumber = stackHead.asInstanceOf[Literal].value.asInstanceOf[Int]
              IsNull(UnresolvedAttribute(schema.fields(fieldNumber).name))
            } else {
              return None
            }
          } else {
            // TODO: error message
            return None
          }

        case INVOKESTATIC =>
          val target = getInvokeMethodTarget(pos)
          val numParameters = target.getParameterTypes.length
          val attributes =
            (stackHeight + 1 - numParameters until stackHeight + 1).map(i => exprs(stack(i)))
          assert(attributes.length == numParameters)
          analyzeMethod(target, schema, attributes) match {
            case Some(expr) => expr
            case None =>
              println("ERROR: Problem analyzing static method call")
              return None
          }

        case INVOKEVIRTUAL =>
          val target = getInvokeMethodTarget(pos)
          val numParameters = target.getParameterTypes.length
          val attributes = (stackHeight - numParameters to stackHeight).map(i => exprs(stack(i)))
          assert(attributes.length == numParameters + 1)
          if (target.getDeclaringClass.getName == classOf[Tuple2[_, _]].getName) {
            if (target.getName == "_2$mcI$sp") {
              UnresolvedAttribute("_2")
            } else {
              println(s"Error: unknown target $target")
              return None
            }
          } else {
            analyzeMethod(target, schema, attributes) match {
              case Some(expr) => expr
              case None =>
                println("ERROR: Problem analyzing method call")
                return None
            }
          }

        case GETFIELD =>
          val target = classPool.get(constPool.getFieldrefClassName(instructions.u16bitAt(pos + 1)))
          val targetField = constPool.getFieldrefName(instructions.u16bitAt(pos + 1))
          if (target.getName == "scala.Tuple2") {
            UnresolvedAttribute(targetField)
          } else {
            println(s"ERROR: Unknown GETFIELD target: $target")
            return None
          }
        case DRETURN | IRETURN | LRETURN | ARETURN =>
          return Some(exprs(stack(stackHeight)))
        case _ =>
          println(s"ERROR: Unknown opcode $mnemonic")
          return None
      }
      stackHeight += stackGrow

      exprs.toSeq.sortBy(_._1).foreach { case (label, value) =>
        println(s"    $label = $value")
      }
    }
    throw new Exception("oh no!")
  }

  def isStatic(method: CtMethod): Boolean = Modifier.isStatic(method.getModifiers)

  // TODO: handle argument types
  // For now, this assumes f: Row => Expr
  def convert(closure: Object, schema: StructType): Option[Expression] = {
    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the first apply() method which can be resolved to an expression
    applyMethods.flatMap { method =>
      println(" \n  " * 10)
      assert(method.getParameterTypes.length == 1)
      val attributes = Seq(UnresolvedAttribute("inputRow"))
      if (isStatic(method)) {
        analyzeMethod(method, schema, attributes)
      } else {
        analyzeMethod(method, schema, Seq(UnresolvedAttribute("this")) ++ attributes)
      }
    }.headOption
  }

  def convertFilter(closure: Object, schema: StructType): Option[Expression] = {
    convert(closure, schema).map { expr => Cast(expr, BooleanType) }
  }

}

/**
 * An expression that tests if the given expression can be casted to a type.
 */
case class CheckCast(left: Expression, right: Expression)
    extends BinaryExpression with ExpectsInputTypes with NonSQLExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(left.dataType, StringType)

  override def eval(input: InternalRow): Any = {
    val result = left.eval(input)
    val castToType = Utils.classForName(right.eval(input).asInstanceOf[UTF8String].toString)
    if (result.getClass.isAssignableFrom(castToType)) {
      result
    } else {
      new ClassCastException
    }
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val eval = left.gen(ctx)
    val castToTypeName = right.gen(ctx)
    s"""
      ${eval.code}
      ${castToTypeName.code}
      (${castToTypeName.value})(${eval.value});
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
      """
  }
}

/**
 * An expression that throws NullPointerException on null input values.
 */
case class NPEOnNull(child: Expression) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) throw new NullPointerException
    result
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val eval = child.gen(ctx)
    s"""
      ${eval.code}
      if(${eval.isNull}) { throw new NullPointerException(); }
      ${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
      """
  }
}
