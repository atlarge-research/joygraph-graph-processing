package io.joygraph.core.util;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferUtil {
    public static Field ADDRESS_FIELD = null;
    public static Field CAPACITY_FIELD = null;
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
            CAPACITY_FIELD = Buffer.class.getDeclaredField("capacity");
            CAPACITY_FIELD.setAccessible(true);
        } catch (Throwable t){
                // Failed to access the address field.
                CAPACITY_FIELD = null;
        }
    }

}
