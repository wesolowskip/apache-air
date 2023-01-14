import { PredictionLevels } from "./prediction-levels";

export class PollutionForecast {
    public longtitude: number;
    public lattitude: number;
    public station_name: string;
    public timestamp: Date;
    //public particle: string;
    public prediction: PredictionLevels;
}