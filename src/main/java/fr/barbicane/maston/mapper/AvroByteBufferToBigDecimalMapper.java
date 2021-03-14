package fr.barbicane.maston.mapper;

import io.vavr.Function3;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class AvroByteBufferToBigDecimalMapper implements Function3<ByteBuffer, Schema, String, BigDecimal> {

  @Override
  public BigDecimal apply(ByteBuffer byteBuffer, Schema classSchema, String fieldName) {
    final String typeName = classSchema.getField(fieldName).schema().getType().getName();
    if (Schema.Type.UNION.getName().equals(typeName)) {
      return classSchema.getField(fieldName).schema().getTypes().stream()
          .filter(schema -> schema.getType().getName().equals(Schema.Type.BYTES.getName()))
          .findFirst()
          .map(schema -> new Conversions.DecimalConversion().fromBytes(byteBuffer, null, schema.getLogicalType()))
          .orElse(BigDecimal.ZERO);
    } else if (typeName.equals(Schema.Type.BYTES.getName())) {
      final LogicalType logicalType = classSchema.getField(fieldName).schema().getLogicalType();
      return new Conversions.DecimalConversion().fromBytes(byteBuffer, null, logicalType);
    }
    return BigDecimal.ZERO;
  }

}
