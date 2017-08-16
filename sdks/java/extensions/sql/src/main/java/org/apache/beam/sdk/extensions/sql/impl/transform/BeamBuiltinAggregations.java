/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.transform;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Iterator;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG.
 */
class BeamBuiltinAggregations {
  /**
   * {@link CombineFn} for MAX based on {@link Max} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createMax(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Max.ofIntegers();
      case SMALLINT:
        return new CustMax<Short>();
      case TINYINT:
        return new CustMax<Byte>();
      case BIGINT:
        return Max.ofLongs();
      case FLOAT:
        return new CustMax<Float>();
      case DOUBLE:
        return Max.ofDoubles();
      case TIMESTAMP:
        return new CustMax<Date>();
      case DECIMAL:
        return new CustMax<BigDecimal>();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in MAX", fieldType));
    }
  }

  /**
   * {@link CombineFn} for MAX based on {@link Min} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createMin(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Min.ofIntegers();
      case SMALLINT:
        return new CustMin<Short>();
      case TINYINT:
        return new CustMin<Byte>();
      case BIGINT:
        return Min.ofLongs();
      case FLOAT:
        return new CustMin<Float>();
      case DOUBLE:
        return Min.ofDoubles();
      case TIMESTAMP:
        return new CustMin<Date>();
      case DECIMAL:
        return new CustMin<BigDecimal>();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in MIN", fieldType));
    }
  }

  /**
   * {@link CombineFn} for MAX based on {@link Sum} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createSum(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Sum.ofIntegers();
      case SMALLINT:
        return new ShortSum();
      case TINYINT:
        return new ByteSum();
      case BIGINT:
        return Sum.ofLongs();
      case FLOAT:
        return new FloatSum();
      case DOUBLE:
        return Sum.ofDoubles();
      case DECIMAL:
        return new BigDecimalSum();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in SUM", fieldType));
    }
  }

  /**
   * {@link CombineFn} for AVG.
   */
  public static CombineFn createAvg(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return new IntegerAvg();
      case SMALLINT:
        return new ShortAvg();
      case TINYINT:
        return new ByteAvg();
      case BIGINT:
        return new LongAvg();
      case FLOAT:
        return new FloatAvg();
      case DOUBLE:
        return new DoubleAvg();
      case DECIMAL:
        return new BigDecimalAvg();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in AVG", fieldType));
    }
  }

  /**
   * {@link CombineFn} for VAR_POP and VAR_SAMP
   */
  public static CombineFn createVar(SqlTypeName fieldType, boolean isSamp) {
    switch (fieldType) {
      case INTEGER:
        return new IntegerVar(isSamp);
      case SMALLINT:
        return new ShortVar(isSamp);
      case TINYINT:
        return new ByteVar(isSamp);
      case BIGINT:
        return new LongVar(isSamp);
      case FLOAT:
        return new FloatVar(isSamp);
      case DOUBLE:
        return new DoubleVar(isSamp);
      case DECIMAL:
        return new BigDecimalVar(isSamp);
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in AVG", fieldType));
    }
  }

  static class CustMax<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    public T apply(T left, T right) {
      return (right == null || right.compareTo(left) < 0) ? left : right;
    }
  }

  static class CustMin<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    public T apply(T left, T right) {
      return (left == null || left.compareTo(right) < 0) ? left : right;
    }
  }

  static class ShortSum extends Combine.BinaryCombineFn<Short> {
    public Short apply(Short left, Short right) {
      return (short) (left + right);
    }
  }

  static class ByteSum extends Combine.BinaryCombineFn<Byte> {
    public Byte apply(Byte left, Byte right) {
      return (byte) (left + right);
    }
  }

  static class FloatSum extends Combine.BinaryCombineFn<Float> {
    public Float apply(Float left, Float right) {
      return left + right;
    }
  }

  static class BigDecimalSum extends Combine.BinaryCombineFn<BigDecimal> {
    public BigDecimal apply(BigDecimal left, BigDecimal right) {
      return left.add(right);
    }
  }

  /**
   * {@link CombineFn} for <em>AVG</em> on {@link Number} types.
   */
  abstract static class Avg<T extends Number>
      extends CombineFn<T, KV<Integer, BigDecimal>, T> {
    @Override
    public KV<Integer, BigDecimal> createAccumulator() {
      return KV.of(0, new BigDecimal(0));
    }

    @Override
    public KV<Integer, BigDecimal> addInput(KV<Integer, BigDecimal> accumulator, T input) {
      return KV.of(accumulator.getKey() + 1, accumulator.getValue().add(toBigDecimal(input)));
    }

    @Override
    public KV<Integer, BigDecimal> mergeAccumulators(
        Iterable<KV<Integer, BigDecimal>> accumulators) {
      int size = 0;
      BigDecimal acc = new BigDecimal(0);
      Iterator<KV<Integer, BigDecimal>> ite = accumulators.iterator();
      while (ite.hasNext()) {
        KV<Integer, BigDecimal> ele = ite.next();
        size += ele.getKey();
        acc = acc.add(ele.getValue());
      }
      return KV.of(size, acc);
    }

    @Override
    public Coder<KV<Integer, BigDecimal>> getAccumulatorCoder(CoderRegistry registry,
        Coder<T> inputCoder) throws CannotProvideCoderException {
      return KvCoder.of(BigEndianIntegerCoder.of(), BigDecimalCoder.of());
    }

    public abstract T extractOutput(KV<Integer, BigDecimal> accumulator);
    public abstract BigDecimal toBigDecimal(T record);
  }

  static class IntegerAvg extends Avg<Integer>{
    public Integer extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).intValue();
    }

    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class LongAvg extends Avg<Long>{
    public Long extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).longValue();
    }

    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class ShortAvg extends Avg<Short>{
    public Short extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).shortValue();
    }

    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteAvg extends Avg<Byte>{
    public Byte extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).byteValue();
    }

    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class FloatAvg extends Avg<Float>{
    public Float extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).floatValue();
    }

    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleAvg extends Avg<Double>{
    public Double extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).doubleValue();
    }

    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalAvg extends Avg<BigDecimal>{
    public BigDecimal extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null
          : accumulator.getValue().divide(new BigDecimal(accumulator.getKey()));
    }

    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }

  static class VarAgg implements Serializable {
    long count; // number of elements
    double sum; // sum of elements

    public VarAgg(long count, double sum) {
      this.count = count;
      this.sum = sum;
   }
  }

  /**
   * {@link CombineFn} for <em>VarPop</em> on {@link Number} types.
   */
  abstract static class VarPop<T extends Number>
          extends CombineFn<T, KV<BigDecimal, VarAgg>, T> {
    @Override
    public KV<BigDecimal, VarAgg> createAccumulator() {
      VarAgg varagg = new VarAgg(0L, 0);
      return KV.of(new BigDecimal(0), varagg);
    }

    @Override
    public KV<BigDecimal, VarAgg> addInput(KV<BigDecimal, VarAgg> accumulator, T input) {
      double v;
      if (input == null) {
        return accumulator;
      } else {
        v = new BigDecimal(input.toString()).doubleValue();
        accumulator.getValue().count++;
        accumulator.getValue().sum += v;
        double variance;
        if (accumulator.getValue().count > 1) {
          double t = accumulator.getValue().count * v - accumulator.getValue().sum;
          variance = (t * t) / (accumulator.getValue().count * (accumulator.getValue().count - 1));
        } else {
          variance = 0;
        }
       return KV.of(accumulator.getKey().add(new BigDecimal(variance)), accumulator.getValue());
      }
    }

    @Override
    public KV<BigDecimal, VarAgg> mergeAccumulators(
            Iterable<KV<BigDecimal, VarAgg>> accumulators) {
      BigDecimal variance = new BigDecimal(0);
      long count = 0;
      double sum = 0;

      Iterator<KV<BigDecimal, VarAgg>> ite = accumulators.iterator();
      while (ite.hasNext()) {
        KV<BigDecimal, VarAgg> r = ite.next();

        double b = r.getValue().sum;

        count += r.getValue().count;
        sum += b;

        double t = (r.getValue().count / (double) count) * sum - b;
        double d = t * t
                * ((count / (double) r.getValue().count) / ((double) count + r.getValue().count));
        variance = variance.add(r.getKey().add(new BigDecimal(d)));
      }

      return KV.of(variance, new VarAgg(count, sum));
    }

    @Override
    public Coder<KV<BigDecimal, VarAgg>> getAccumulatorCoder(CoderRegistry registry,
        Coder<T> inputCoder) throws CannotProvideCoderException {
      return KvCoder.of(BigDecimalCoder.of(), SerializableCoder.of(VarAgg.class));
    }

    public abstract T extractOutput(KV<BigDecimal, VarAgg> accumulator);
    public abstract BigDecimal toBigDecimal(T record);
  }

  static class IntegerVar extends VarPop<Integer> {
    boolean isSamp;

    public IntegerVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Integer extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.intValue();
    }

    @Override
    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class ShortVar extends VarPop<Short> {
    boolean isSamp;

    public ShortVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Short extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.shortValue();
    }

    @Override
    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteVar extends VarPop<Byte> {
    boolean isSamp;

    public ByteVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Byte extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.byteValue();
    }

    @Override
    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class LongVar extends VarPop<Long> {
    boolean isSamp;

    public LongVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Long extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.longValue();
    }

    @Override
    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class FloatVar extends VarPop<Float> {
    boolean isSamp;

    public FloatVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Float extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.floatValue();
    }

    @Override
    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleVar extends VarPop<Double> {
    boolean isSamp;

    public DoubleVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public Double extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                5, RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar.doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalVar extends VarPop<BigDecimal> {
    boolean isSamp;

    public BigDecimalVar(boolean isSamp) {
      this.isSamp = isSamp;
    }

    public BigDecimal extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      BigDecimal decimalVar = null;
      if (accumulator.getValue().count > 1) {
        decimalVar = accumulator.getKey().divide(
                new BigDecimal(accumulator.getValue().count - (this.isSamp ? 1 : 0)),
                RoundingMode.HALF_UP);
      } else {
        decimalVar = new BigDecimal(0);
      }

      return decimalVar;
    }

    @Override
    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }
}
