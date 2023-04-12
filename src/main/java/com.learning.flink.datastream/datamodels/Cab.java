package com.learning.flink.datastream.datamodels;


import java.io.Serializable;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

//cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count


@Nonnull
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Cab implements Serializable {
    private static final long serialVersionUID = 1L;
    private String cabId;
    private String numberPlate;
    private String cabType;
    private String cabDriverName;
    private String onGoing;
    private String pickupLocation;
    private String destination;
    private Integer passengerCount;
    private long timeStamp;
}
