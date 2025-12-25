const d=JSON.parse(require('fs').readFileSync(0,'utf8'));
const rows = d.data?.rows || [];
const cols = d.data?.cols?.map(c => c.name) || [];

console.log('=== АНАЛИЗ ВИТРИНЫ: Отчеты участников Technopark ===');
console.log('Всего записей:', rows.length);
console.log('');

// Индексы колонок
const yearIdx = cols.indexOf('year');
const reportTypeIdx = cols.indexOf('report_type');
const incomeTotalIdx = cols.indexOf('income_total');
const taxIncentivesIdx = cols.indexOf('tax_incentives');
const residentsIdx = cols.indexOf('residents_count');
const nonresidentsIdx = cols.indexOf('nonresidents_count');
const incomeIntlIdx = cols.indexOf('income_international');

// Анализ по годам
const byYear = {};
rows.forEach(r => {
    const y = r[yearIdx];
    if (!byYear[y]) byYear[y] = { count: 0, income: 0, tax: 0, residents: 0 };
    byYear[y].count++;
    byYear[y].income += r[incomeTotalIdx] || 0;
    byYear[y].tax += r[taxIncentivesIdx] || 0;
    byYear[y].residents += r[residentsIdx] || 0;
});

console.log('По годам:');
Object.keys(byYear).sort().reverse().forEach(y => {
    const d = byYear[y];
    console.log('  ' + y + ': ' + d.count + ' отчетов, доход ' + (d.income/1e9).toFixed(2) + ' млрд, налог.льготы ' + (d.tax/1e9).toFixed(2) + ' млрд');
});

// Анализ по типам отчетов
const byType = {};
rows.forEach(r => {
    const t = r[reportTypeIdx];
    if (!byType[t]) byType[t] = 0;
    byType[t]++;
});
console.log('');
console.log('По типам отчетов:');
Object.keys(byType).forEach(t => console.log('  ' + t + ': ' + byType[t]));

// Общая статистика
let totalIncome = 0, totalTax = 0, totalIntl = 0, totalResidents = 0;
rows.forEach(r => {
    totalIncome += r[incomeTotalIdx] || 0;
    totalTax += r[taxIncentivesIdx] || 0;
    totalIntl += r[incomeIntlIdx] || 0;
    totalResidents += r[residentsIdx] || 0;
});

console.log('');
console.log('Итого по всем отчетам:');
console.log('  Совокупный доход: ' + (totalIncome/1e9).toFixed(2) + ' млрд тенге');
console.log('  Международный доход: ' + (totalIntl/1e9).toFixed(2) + ' млрд тенге');
console.log('  Налоговые льготы: ' + (totalTax/1e9).toFixed(2) + ' млрд тенге');
console.log('  Всего резидентов: ' + totalResidents.toLocaleString());
