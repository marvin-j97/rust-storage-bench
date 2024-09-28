// Taken from https://www.npmjs.com/package/combinate, is not ESM

function combinate<O extends Record<string | number, any[]>>(obj: O) {
  let combos: { [k in keyof O]: O[k][number] }[] = [];

  for (var key in obj) {
    const values = obj[key];
    const all = [];
    for (let i = 0; i < values.length; i++) {
      for (let j = 0; j < (combos.length || 1); j++) {
        const newCombo = { ...combos[j], [key]: values[i] };
        
        // @ts-ignore
        all.push(newCombo);
      }
    }
    combos = all;
  }
  
  return combos;
}

export default combinate;
