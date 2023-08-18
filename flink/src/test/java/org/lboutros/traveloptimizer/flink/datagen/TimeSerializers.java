package org.lboutros.traveloptimizer.flink.datagen;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.time.*;

public class TimeSerializers {

    public static class LocalDateTimeSerializer extends Serializer<LocalDateTime> {
        public void write(Kryo kryo, Output out, LocalDateTime dateTime) {
            LocalDateSerializer.write(out, dateTime.toLocalDate());
            LocalTimeSerializer.write(out, dateTime.toLocalTime());
        }

        public LocalDateTime read(Kryo kryo, Input in, Class type) {
            LocalDate date = LocalDateSerializer.read(in);
            LocalTime time = LocalTimeSerializer.read(in);
            return LocalDateTime.of(date, time);
        }
    }

    public static class LocalDateSerializer extends Serializer<LocalDate> {
        static void write(Output out, LocalDate date) {
            out.writeInt(date.getYear(), true);
            out.writeByte(date.getMonthValue());
            out.writeByte(date.getDayOfMonth());
        }

        static LocalDate read(Input in) {
            int year = in.readInt(true);
            int month = in.readByte();
            int dayOfMonth = in.readByte();
            return LocalDate.of(year, month, dayOfMonth);
        }

        public void write(Kryo kryo, Output out, LocalDate date) {
            write(out, date);
        }

        public LocalDate read(Kryo kryo, Input in, Class type) {
            return read(in);
        }
    }


    public static class ZonedDateTimeSerializer extends Serializer<ZonedDateTime> {
        public void write(Kryo kryo, Output out, ZonedDateTime obj) {
            LocalDateSerializer.write(out, obj.toLocalDate());
            LocalTimeSerializer.write(out, obj.toLocalTime());
            ZoneIdSerializer.write(out, obj.getZone());
        }

        public ZonedDateTime read(Kryo kryo, Input in, Class type) {
            LocalDate date = LocalDateSerializer.read(in);
            LocalTime time = LocalTimeSerializer.read(in);
            ZoneId zone = ZoneIdSerializer.read(in);
            return ZonedDateTime.of(date, time, zone);
        }
    }

    public static class LocalTimeSerializer extends Serializer<LocalTime> {
        static void write(Output out, LocalTime time) {
            if (time.getNano() == 0) {
                if (time.getSecond() == 0) {
                    if (time.getMinute() == 0) {
                        out.writeByte(~time.getHour());
                    } else {
                        out.writeByte(time.getHour());
                        out.writeByte(~time.getMinute());
                    }
                } else {
                    out.writeByte(time.getHour());
                    out.writeByte(time.getMinute());
                    out.writeByte(~time.getSecond());
                }
            } else {
                out.writeByte(time.getHour());
                out.writeByte(time.getMinute());
                out.writeByte(time.getSecond());
                out.writeInt(time.getNano(), true);
            }
        }

        static LocalTime read(Input in) {
            int hour = in.readByte();
            int minute = 0;
            int second = 0;
            int nano = 0;
            if (hour < 0) {
                hour = ~hour;
            } else {
                minute = in.readByte();
                if (minute < 0) {
                    minute = ~minute;
                } else {
                    second = in.readByte();
                    if (second < 0) {
                        second = ~second;
                    } else {
                        nano = in.readInt(true);
                    }
                }
            }
            return LocalTime.of(hour, minute, second, nano);
        }

        public void write(Kryo kryo, Output out, LocalTime time) {
            write(out, time);
        }

        public LocalTime read(Kryo kryo, Input in, Class type) {
            return read(in);
        }
    }

    public static class ZoneIdSerializer extends Serializer<ZoneId> {
        static void write(Output out, ZoneId obj) {
            out.writeString(obj.getId());
        }

        static ZoneId read(Input in) {
            String id = in.readString();
            return ZoneId.of(id);
        }

        public void write(Kryo kryo, Output out, ZoneId obj) {
            write(out, obj);
        }

        public ZoneId read(Kryo kryo, Input in, Class type) {
            return read(in);
        }
    }


}