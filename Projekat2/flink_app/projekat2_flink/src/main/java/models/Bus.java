package models;
import lombok.*;

import java.text.DecimalFormat;

@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class Bus {
    final String Date;
    final String Time;
    final String BusLine;
    final String BusID;
    final Double Latitude;
    final Double Longitude;
    final Double Speed;
    final Integer Timestamp;

    public Double getLatitudeRounded(int brojDecimala){
        String formatString = "#.";
        for(int i=0; i<brojDecimala; i++)
        {
            formatString+="#";
        }

        DecimalFormat df = new DecimalFormat(formatString);
        return Double.parseDouble(df.format(Latitude));
    }
    public Double getLongitudeRounded(int brojDecimala){
        String formatString = "#.";
        for(int i=0; i<brojDecimala; i++)
        {
            formatString+="#";
        }

        DecimalFormat df = new DecimalFormat(formatString);
        return Double.parseDouble(df.format(Longitude));
    }
}

