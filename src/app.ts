import express from 'express';
import { Pool } from 'pg';

const app = express();
const port = 3000;

const pool = new Pool({
  connectionString:
    'postgres://hvyvudcn:xvQqbJ22KEb7auJxdvpPyj5kbC@dontpanic.k42.app/postgres',
});

const mainQuery = `
  SELECT
    jsonb_build_object(
      'id', c.id,
      'name', c.name,
      'gender', c.gender,
      'ability', c.ability,
      'minimal_distance', c.minimal_distance,
      'weight', c.weight,
      'born', c.born,
      'in_space_since', c.in_space_since,
      'beer_consumption', c.beer_consumption,
      'knows_the_answer', c.knows_the_answer,
      'has_nemesis', COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'id', nemesis_data.nemesis_id,
            'character_id', nemesis_data.character_id,
            'is_alive', nemesis_data.is_alive,
            'years', nemesis_data.years,
            'has_secret', secret_data.secret_data
          )
        ),
        '[]'::jsonb
      )
    ) AS character
  FROM
    character c
  LEFT JOIN (
    SELECT
      n.character_id,
      n.id AS nemesis_id,
      n.is_alive,
      n.years
    FROM
      nemesis n
  ) AS nemesis_data ON c.id = nemesis_data.character_id
  LEFT JOIN (
    SELECT
      s.nemesis_id,
      jsonb_agg(jsonb_build_object(
        'id', s.id,
        'nemesis_id', s.nemesis_id,
        'secret_code', s.secret_code
      )) AS secret_data
    FROM
      secret s
    GROUP BY
      s.nemesis_id
  ) AS secret_data ON nemesis_data.nemesis_id = secret_data.nemesis_id
  GROUP BY
    c.id, c.name, c.gender, c.ability,
    c.minimal_distance, c.weight, c.born,
    c.in_space_since, c.beer_consumption, c.knows_the_answer;
`;

const statisticsQuery = `
  WITH CharacterData AS (
    SELECT
      COUNT(DISTINCT id) AS characters_count,
      COUNT(DISTINCT CASE WHEN gender IN ('M', 'male', 'm') THEN id END) AS male_count,
      COUNT(DISTINCT CASE WHEN gender IN ('F', 'female', 'f') THEN id END) AS female_count,
      COUNT(DISTINCT CASE WHEN gender IS NULL OR gender NOT IN ('M', 'male', 'm', 'F', 'female', 'f') THEN id END) AS other_count,
      AVG(EXTRACT(YEAR FROM AGE(NOW(), born))) AS average_age,
      AVG(weight) AS average_weight
    FROM
      character
  ),
  NemesisData AS (
    SELECT
      COUNT(DISTINCT id) AS nemesis_count,
      AVG(n.years) AS average_nemesis_age
    FROM
      nemesis n
  )
  SELECT
    characters_count,
    male_count,
    female_count,
    other_count,
    average_age,
    average_weight,
    nemesis_count,
    average_nemesis_age,
    (characters_count + nemesis_count) AS overall_count,
    ((characters_count * average_age + nemesis_count * average_nemesis_age) / (characters_count + nemesis_count)) AS overall_average_age
  FROM
    CharacterData, NemesisData;
`;

app.get('/', async (req, res) => {
  try {
    const responseData = await getData();
    res.json(responseData);
  } catch (error: any) {
    console.error('Error handling request:', error.message);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

async function getData() {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const [characterDataResult, statisticsResult] = await Promise.all([
      client.query(mainQuery),
      client.query(statisticsQuery),
    ]);

    await client.query('COMMIT');

    const responseData = {
      characters_count: statisticsResult.rows[0].characters_count,
      nemesis_count: statisticsResult.rows[0].nemesis_count,
      average_character_age: statisticsResult.rows[0].average_age,
      average_nemesis_age: statisticsResult.rows[0].average_nemesis_age,
      average_age_overall: statisticsResult.rows[0].overall_average_age,
      average_character_weight: statisticsResult.rows[0].average_weight,
      genders: {
        female: statisticsResult.rows[0].female_count,
        male: statisticsResult.rows[0].male_count,
        other: statisticsResult.rows[0].other_count,
      },
      data: characterDataResult.rows,
    };

    return responseData;
  } catch (error: any) {
    console.error('Error executing query:', error.message);
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
