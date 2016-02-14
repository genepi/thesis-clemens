package util;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.02.16.
 */
public class ChromPosKey {
    private int chromosome;
    private int position;

    public ChromPosKey(int chromosome, int position) {
        this.chromosome = chromosome;
        this.position = position;
    }

    public int getChromosome() {
        return chromosome;
    }

    public void setChromosome(int chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChromPosKey that = (ChromPosKey) o;

        if (chromosome != that.chromosome) return false;
        return position == that.position;

    }

    @Override
    public int hashCode() {
        int result = chromosome;
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString() {
        return chromosome + "\t" + position;
    }

}