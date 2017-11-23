package io.joygraph.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferUtil {
    public static Field ADDRESS_FIELD = null;
    public static Field CAPACITY_FIELD = null;
    public static Field LIMIT_FIELD = null;
    public static Field POSITION_FIELD = null;

    public static Constructor<? extends ByteBuffer> BBCONSTRUCTOR = null;
    public static Field ATTFIELD = null;

    static {
        ByteBuffer direct = ByteBuffer.allocateDirect(1);
        try {
            ADDRESS_FIELD = Buffer.class.getDeclaredField("address");
            ADDRESS_FIELD.setAccessible(true);
            if (ADDRESS_FIELD.getLong(ByteBuffer.allocate(1)) != 0) { // A heap buffer must have 0 address.
                ADDRESS_FIELD = null;
            }
            else if (ADDRESS_FIELD.getLong(direct) == 0) { // A direct buffer must have non-zero address.
                ADDRESS_FIELD = null;
            }
        } catch (Throwable t) {
            // Failed to access the address field.
            ADDRESS_FIELD = null;
        }

        try {
            LIMIT_FIELD = Buffer.class.getDeclaredField("limit");
            LIMIT_FIELD.setAccessible(true);
        } catch (Throwable t){
                // Failed to access the address field.
            LIMIT_FIELD = null;
        }

        try {
            POSITION_FIELD = Buffer.class.getDeclaredField("position");
            POSITION_FIELD.setAccessible(true);
        } catch (Throwable t){
            // Failed to access the address field.
            POSITION_FIELD = null;
        }

        try {
            CAPACITY_FIELD = Buffer.class.getDeclaredField("capacity");
            CAPACITY_FIELD.setAccessible(true);
        } catch (Throwable t){
            // Failed to access the address field.
            CAPACITY_FIELD = null;
        }

        try {
            ATTFIELD = direct.getClass().getDeclaredField("att");
            ATTFIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            System.out.println("fuck me");
            ATTFIELD = null;
        }

        try {
            BBCONSTRUCTOR = direct.getClass().getDeclaredConstructor(Long.TYPE, Integer.TYPE, Object.class);
            BBCONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            BBCONSTRUCTOR = null;
        }
    }

}
